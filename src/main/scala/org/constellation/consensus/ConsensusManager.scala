package org.constellation.consensus

import org.constellation.domain.exception.InvalidNodeState
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO, LiftIO, Sync}
import cats.implicits._
import com.typesafe.config.Config
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointAcceptanceService, CheckpointService}
import org.constellation.consensus.Consensus._
import org.constellation.domain.observation.{Observation, ObservationService}
import org.constellation.p2p.{Cluster, DataResolver, PeerData, PeerNotification}
import org.constellation.primitives.Schema.{CheckpointCache, NodeState, NodeType, SignedObservationEdgeCache}
import org.constellation.domain.schema.Id
import org.constellation.domain.transaction.TransactionService
import org.constellation.primitives.concurrency.{SingleLock, SingleRef}
import org.constellation.primitives.{ChannelMessage, CheckpointBlock, ConcurrentTipService, Transaction}
import org.constellation.storage._
import org.constellation.util.{Distance, PeerApiClient}
import org.constellation.{ConfigUtil, ConstellationExecutionContext, DAO}

import scala.util.Try

class ConsensusManager[F[_]: Concurrent](
  transactionService: TransactionService[F],
  concurrentTipService: ConcurrentTipService[F],
  checkpointService: CheckpointService[F],
  checkpointAcceptanceService: CheckpointAcceptanceService[F],
  soeService: SOEService[F],
  messageService: MessageService[F],
  observationService: ObservationService[F],
  remoteSender: ConsensusRemoteSender[F],
  cluster: Cluster[F],
  dao: DAO,
  config: Config,
  remoteCall: ContextShift[F],
  calculationContext: ContextShift[F]
) {

  import ConsensusManager._

  implicit val shadowDAO: DAO = dao

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private val maxTransactionThreshold: Int =
    ConfigUtil.constellation.getInt("consensus.maxTransactionThreshold")
  private val maxObservationThreshold: Int =
    ConfigUtil.constellation.getInt("consensus.maxObservationThreshold")

  val timeout: Long =
    ConfigUtil.getDurationFromConfig("constellation.consensus.form-checkpoint-blocks-timeout").toMillis

  private val semaphore: Semaphore[F] = {
    implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
    Semaphore.in[IO, F](1).unsafeRunSync()
  }
  private[consensus] val consensuses: SingleRef[F, Map[RoundId, ConsensusInfo[F]]] = SingleRef(
    Map.empty[RoundId, ConsensusInfo[F]]
  )
  private[consensus] val ownConsensus: SingleRef[F, Option[OwnConsensus[F]]] = SingleRef(None)
  private[consensus] val proposals: StorageService[F, List[ConsensusProposal]] =
    new StorageService("ConsensusProposal".some, 2.some)

  private def withLock[R](name: String, thunk: F[R]) = new SingleLock[F, R](name, semaphore).use(thunk)

  def getRound(roundId: RoundId): F[Option[Consensus[F]]] =
    for {
      own <- ownConsensus.get
      maybe <- if (own.exists(_.roundId == roundId)) Sync[F].pure(own.flatMap(_.consensusInfo.map(_.consensus)))
      else
        consensuses.get.map(consensuses => consensuses.get(roundId).map(_.consensus))
    } yield maybe

  def getActiveMinHeight: F[Option[Long]] =
    consensuses.getUnsafe.map(_.flatMap(_._2.tipMinHeight).toList match {
      case Nil  => None
      case list => Some(list.min)
    })

  def startOwnConsensus(): F[ConsensusInfo[F]] = {
    val startRoundTask = for {
      roundId <- withLock("startOwnRound", syncRoundInProgress())
      _ <- logger.debug(s"[${dao.id.short}] Starting own consensus $roundId")
      roundData <- createRoundData(roundId)
      missing <- resolveMissingParents(roundData._1)
      roundInfo = ConsensusInfo[F](
        new Consensus[F](
          roundData._1,
          roundData._2,
          new DataResolver,
          transactionService,
          checkpointAcceptanceService,
          messageService,
          observationService,
          remoteSender,
          this,
          shadowDAO,
          config,
          remoteCall,
          calculationContext
        ),
        roundData._1.tipsSOE.minHeight,
        System.currentTimeMillis()
      )
      _ <- ownConsensus.updateUnsafe(d => d.map(o => o.copy(consensusInfo = roundInfo.some)))
      _ <- logger.debug(s"[${dao.id.short}] created data for round: ${roundId} with facilitators: ${roundData._1.peers
        .map(_.peerMetadata.id.short)}")
      responses <- remoteSender.notifyFacilitators(roundData._1)
      _ <- if (responses.forall(_.isSuccess)) Sync[F].unit
      else
        Sync[F].raiseError[Unit](
          NotAllPeersParticipate(roundId, roundData._1.transactions.map(_.hash), roundData._1.observations)
        )
      _ <- roundInfo.consensus.addTransactionProposal(
        LightTransactionsProposal(
          roundData._1.roundId,
          FacilitatorId(dao.id),
          roundData._1.transactions.map(_.hash),
          roundData._1.messages.map(_.signedMessageData.hash),
          roundData._1.peers.flatMap(_.notification).toSeq,
          roundData._1.observations
        )
      )
    } yield roundInfo

    startRoundTask.recoverWith {
      case error: ConsensusStartError =>
        logger.debug(error.getMessage).flatMap(_ => Sync[F].raiseError[ConsensusInfo[F]](error))
      case error: ConsensusError =>
        logger
          .debug(error.getMessage)
          .flatMap(
            _ =>
              stopBlockCreationRound(
                StopBlockCreationRound(error.roundId, None, error.transactions, error.observations)
              )
          )
          .flatMap(_ => Sync[F].raiseError[ConsensusInfo[F]](error))
      case unknown =>
        logger
          .error(unknown)(s"Unexpected error when starting own consensus: ${unknown.getMessage}")
          .flatMap(_ => Sync[F].raiseError[ConsensusInfo[F]](unknown))
    }
  }

  def syncRoundInProgress(): F[RoundId] =
    for {
      state <- cluster.getNodeState
      _ <- if (NodeState.canStartOwnConsensus(state)) Sync[F].unit
      else Sync[F].raiseError[Unit](InvalidNodeState(NodeState.validForOwnConsensus, state))
      own <- ownConsensus.getUnsafe
      roundId <- if (own.isDefined) Sync[F].raiseError[RoundId](OwnRoundAlreadyInProgress)
      else
        ownConsensus.modify { _ =>
          val id = generateRoundId
          (Some(OwnConsensus(id, None)), id)
        }
    } yield roundId

  def createRoundData(roundId: RoundId): F[(RoundData, Seq[(ChannelMessage, Int)])] =
    for {
      transactions <- transactionService
        .pullForConsensus(maxTransactionThreshold)
      facilitators <- LiftIO[F].liftIO(dao.readyFacilitatorsAsync)
      tips <- concurrentTipService.pull(facilitators)(dao.metrics)
      _ <- if (tips.isEmpty)
        Sync[F]
          .raiseError[Unit](NoTipsForConsensus(roundId, transactions.map(_.transaction.hash), List.empty[Observation]))
      else Sync[F].unit
      _ <- if (tips.get.peers.isEmpty)
        Sync[F]
          .raiseError[Unit](NoPeersForConsensus(roundId, transactions.map(_.transaction.hash), List.empty[Observation]))
      else Sync[F].unit
      messages <- Sync[F].delay(dao.threadSafeMessageMemPool.pull().getOrElse(Seq()))
      observations <- observationService.pullForConsensus(maxObservationThreshold)
      lightNodes <- LiftIO[F].liftIO(dao.readyPeers(NodeType.Light))
      lightPeers = if (lightNodes.isEmpty) Set.empty[PeerData]
      else
        Set(lightNodes.minBy(p => Distance.calculate(transactions.head.transaction.baseHash, p._1))._2) // TODO: Choose more than one tx and light peers
      allFacilitators = tips.get.peers.values.map(_.peerMetadata.id).toSet ++ Set(dao.id)
      arbitraryMsgs <- getArbitraryMessagesWithDistance(allFacilitators).map(_.filter(t => t._2 == 1))
      roundData = (
        RoundData(
          roundId,
          tips.get.peers.values.toSet,
          lightPeers,
          FacilitatorId(dao.id),
          transactions.map(_.transaction),
          tips.get.tipSoe,
          messages,
          observations
        ),
        arbitraryMsgs
      )

    } yield roundData

  def participateInBlockCreationRound(roundData: RoundData): F[(ConsensusInfo[F], RoundData)] =
    for {
      state <- cluster.getNodeState
      _ <- if (NodeState.canParticipateConsensus(state)) Sync[F].unit
      else Sync[F].raiseError[Unit](InvalidNodeState(NodeState.validForConsensusParticipation, state))
      allFacilitators = roundData.peers.map(_.peerMetadata.id) ++ Set(dao.id)
      arbitraryMsgs <- getArbitraryMessagesWithDistance(allFacilitators)
      updatedRoundData <- adjustPeers(roundData)
      roundInfo = ConsensusInfo(
        new Consensus[F](
          updatedRoundData,
          arbitraryMsgs,
          new DataResolver,
          transactionService,
          checkpointAcceptanceService,
          messageService,
          observationService,
          remoteSender,
          this,
          shadowDAO,
          config,
          remoteCall,
          calculationContext
        ),
        roundData.tipsSOE.minHeight,
        System.currentTimeMillis()
      )
      _ <- consensuses.updateUnsafe(r => r + (roundData.roundId -> roundInfo))
      _ <- logger.debug(s"[${dao.id.short}] Participate in round ${updatedRoundData.roundId}")
    } yield (roundInfo, updatedRoundData)

  def continueRoundParticipation(roundInfo: ConsensusInfo[F], roundData: RoundData): F[Unit] =
    for {
      _ <- resolveMissingParents(roundData)
      _ <- withLock(roundData.roundId.toString, passMissed(roundData.roundId, roundInfo.consensus))
      _ <- roundInfo.consensus.startTransactionProposal()
    } yield ()

  def addMissed(roundId: RoundId, roundCommand: ConsensusProposal): F[Unit] =
    withLock(roundId.toString, addMissedUnsafe(roundId, roundCommand))

  private def addMissedUnsafe(roundId: RoundId, roundCommand: ConsensusProposal): F[Unit] =
    for {
      missed <- proposals.lookup(roundId.toString).map(_.toList.flatten)
      _ <- proposals.put(roundId.toString, missed :+ roundCommand)
    } yield ()

  def passMissed(roundId: RoundId, consensus: Consensus[F]): F[Unit] =
    for {
      missed <- proposals.lookup(roundId.toString).map(_.toList.flatten)
      _ <- missed.traverse {
        case proposal: LightTransactionsProposal => consensus.addTransactionProposal(proposal)
        case proposal: SelectedUnionBlock        => consensus.addSelectedBlockProposal(proposal)
        case proposal: UnionBlockProposal        => consensus.addBlockProposal(proposal)
      }
    } yield ()

  def terminateConsensuses(): F[Unit] =
    for {
      _ <- logger.debug(
        s"[${dao.id.short}] Terminating all consensuses"
      )
      _ <- consensuses.set(Map.empty[RoundId, ConsensusInfo[F]])
      _ <- ownConsensus.set(None)
    } yield ()

  def stopBlockCreationRound(cmd: StopBlockCreationRound): F[Unit] =
    for {
      _ <- consensuses.update(curr => curr - cmd.roundId)
      _ <- ownConsensus.update(curr => if (curr.isDefined && curr.get.roundId == cmd.roundId) None else curr)
//      _ <- transactionService.returnToPending(cmd.transactionsToReturn) // TODO: wkoszycki temporally discard transactions/observations
//      _ <- observationService.returnToPending(cmd.observationsToReturn)
      _ <- transactionService.clearInConsensus(cmd.transactionsToReturn)
      _ <- observationService.clearInConsensus(cmd.observationsToReturn.map(_.hash))
      _ <- updateNotifications(cmd.maybeCB.map(_.notifications.toList))
      _ = releaseMessages(cmd.maybeCB)
      _ <- logger.debug(
        s"[${dao.id.short}] Consensus stopped ${cmd.roundId} with block: ${cmd.maybeCB.map(_.baseHash).getOrElse("empty")}"
      )
    } yield ()

  def updateNotifications(notifications: Option[List[PeerNotification]]): F[Unit] =
    notifications match {
      case None      => Sync[F].unit
      case Some(Nil) => Sync[F].unit
      case Some(nonEmpty) =>
        cluster.updatePeerNotifications(nonEmpty)
    }

  def releaseMessages(maybeCB: Option[CheckpointBlock]): Unit =
    maybeCB.foreach(
      cb =>
        cb.messages.foreach(
          message =>
            dao.threadSafeMessageMemPool.activeChannels
              .get(message.signedMessageData.data.channelId)
              .foreach(_.release())
        )
    )

  def cleanUpLongRunningConsensus: F[Unit] =
    for {
      runningConsensuses <- consensuses.getUnsafe
      currentTime <- Sync[F].delay(System.currentTimeMillis())
      ownRound <- ownConsensus.getUnsafe.map(_.flatMap(o => o.consensusInfo.map(i => o.roundId -> i)))
      toClean = (runningConsensuses ++ ownRound.toMap).filter(r => (currentTime - r._2.startTime) > timeout).toList
      stopData <- toClean.traverse(
        r =>
          r._2.consensus.getOwnTransactionsToReturn
            .flatMap(txs => r._2.consensus.getOwnObservationsToReturn.map(exs => (r._1, txs, exs)))
      )
      _ <- if (stopData.nonEmpty) logger.warn(s"Cleaning timeout consensuses with roundId: ${stopData.map(_._1)}")
      else Sync[F].unit
      _ <- stopData.traverse(s => stopBlockCreationRound(StopBlockCreationRound(s._1, None, s._2, s._3)))
    } yield ()

  def handleRoundError(cmd: ConsensusException): F[Unit] =
    for {
      _ <- logger.error(cmd)(s"Consensus with roundId: ${cmd.roundId} finished with error: ${cmd.getMessage}")
      _ <- stopBlockCreationRound(
        StopBlockCreationRound(cmd.roundId, None, cmd.transactionsToReturn, cmd.observationsToReturn)
      )
    } yield ()

  private[consensus] def adjustPeers(roundData: RoundData): F[RoundData] =
    cluster.getPeerInfo.map { peers =>
      val initiator = peers.get(roundData.facilitatorId.id) match {
        case Some(value) => value
        case None =>
          throw new IllegalStateException(
            s"Unable to find round initiator for round ${roundData.roundId} and facilitatorId: ${roundData.facilitatorId}"
          )
      }
      roundData.copy(
        peers = roundData.peers
          .filter(_.peerMetadata.id != dao.id) + initiator
      )
    }

  private[consensus] def resolveMissingParents(
    roundData: RoundData
  )(implicit dao: DAO): F[List[CheckpointCache]] = {
    def resolve(hash: String, peer: Option[PeerApiClient]): F[CheckpointCache] =
      LiftIO[F].liftIO(
        DataResolver.resolveCheckpointDefaults(hash, peer)(IO.contextShift(ConstellationExecutionContext.bounded))(
          dao = shadowDAO
        )
      )

    for {
      _ <- roundData.tipsSOE.soe.toList.traverse(
        soe => soeService.put(soe.hash, SignedObservationEdgeCache(soe, resolved = true))
      )
      filtered <- roundData.tipsSOE.soe.toList.traverse(
        t =>
          checkpointService
            .contains(t.baseHash)
            .map(exist => if (!exist) t.baseHash.some else None)
      )
      resolved <- filtered.flatten match {
        case Nil => Sync[F].pure[List[CheckpointCache]](List.empty)
        case nel =>
          val peers = roundData.peers.map(p => PeerApiClient(p.peerMetadata.id, p.client))
          nel.traverse(resolve(_, peers.find(_.id == roundData.facilitatorId.id)))
      }
      _ <- logger.debug(
        s"[${dao.id.short}] Resolved missing parents size: ${resolved.size} for round ${roundData.roundId}"
      )
    } yield resolved
  }

  def getArbitraryMessagesWithDistance(facilitators: Set[Id]): F[Seq[(ChannelMessage, Int)]] = {

    def measureDistance(id: Id, tx: ChannelMessage): BigInt = Distance.calculate(tx.signedMessageData.hash, id)

    messageService.arbitraryPool
      .toMap()
      .map(_.map { m =>
        (
          m._2.channelMessage,
          facilitators
            .map(f => (f, measureDistance(f, m._2.channelMessage)))
            .toSeq
            .sortBy(_._2)
            .map(_._1)
            .indexOf(dao.id)
        )
      }.toSeq)
  }

}
case class OwnConsensus[F[_]: Concurrent](
  roundId: RoundId,
  consensusInfo: Option[ConsensusInfo[F]] = None
)

case class ConsensusInfo[F[_]: Concurrent](
  consensus: Consensus[F],
  tipMinHeight: Option[Long],
  startTime: Long
)

object ConsensusManager {

  def generateRoundId: RoundId =
    RoundId(java.util.UUID.randomUUID().toString)

  case class BroadcastLightTransactionProposal(
    roundId: RoundId,
    peers: Set[PeerData],
    transactionsProposal: LightTransactionsProposal
  )

  case object OwnRoundAlreadyInProgress extends ConsensusStartError("Node has already start own consensus")

  class ConsensusStartError(message: String) extends Exception(message)

  class ConsensusError(
    val roundId: RoundId,
    val transactions: List[String],
    val observations: List[Observation],
    message: String
  ) extends Exception(message)

  case class NoTipsForConsensus(id: RoundId, txs: List[String], obs: List[Observation])
      extends ConsensusError(id, txs, obs, s"No tips to start consensus $id")
  case class NoPeersForConsensus(id: RoundId, txs: List[String], obs: List[Observation])
      extends ConsensusError(id, txs, obs, s"No active peers to start consensus $id")
  case class NotAllPeersParticipate(id: RoundId, txs: List[String], obs: List[Observation])
      extends ConsensusError(id, txs, obs, s"Not all of the peers has participated in consensus $id")

  case class BroadcastUnionBlockProposal(roundId: RoundId, peers: Set[PeerData], proposal: UnionBlockProposal)
  case class BroadcastSelectedUnionBlock(roundId: RoundId, peers: Set[PeerData], cb: SelectedUnionBlock)
  case class ConsensusTimeout(roundId: RoundId)

  case class SnapshotHeightAboveTip(id: RoundId, snapHeight: Long, tipHeight: Long)
      extends Exception(
        s"Can't participate in round $id snapshot height: $snapHeight is above or/equal proposed tip $tipHeight"
      )
}
