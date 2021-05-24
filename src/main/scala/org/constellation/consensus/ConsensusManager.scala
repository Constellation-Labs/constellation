package org.constellation.consensus

import org.constellation.domain.exception.InvalidNodeState
import cats.effect.concurrent.{Ref, Semaphore}
import cats.effect.{Blocker, Concurrent, ContextShift, IO, LiftIO, Sync, Timer}
import cats.syntax.all._
import com.typesafe.config.Config
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.ConstellationExecutionContext.createSemaphore
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.Consensus._
import org.constellation.domain.observation.ObservationService
import org.constellation.p2p.{Cluster, DataResolver, PeerData}
import org.constellation.domain.transaction.TransactionService
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.concurrency.SingleLock
import org.constellation.gossip.checkpoint.CheckpointBlockGossipService
import org.constellation.schema.checkpoint.{CheckpointBlock, CheckpointCache}
import org.constellation.schema.consensus.RoundId
import org.constellation.schema.observation.Observation
import org.constellation.schema.transaction.Transaction
import org.constellation.schema.{ChannelMessage, Id, NodeState, NodeType, PeerNotification}
import org.constellation.storage._
import org.constellation.util.{Distance, Metrics}
import org.constellation.{ConfigUtil, DAO}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Try

class ConsensusManager[F[_]: Concurrent: ContextShift: Timer](
  transactionService: TransactionService[F],
  checkpointService: CheckpointService[F],
  messageService: MessageService[F],
  observationService: ObservationService[F],
  remoteSender: ConsensusRemoteSender[F],
  cluster: Cluster[F],
  apiClient: ClientInterpreter[F],
  dataResolver: DataResolver[F],
  checkpointBlockGossipService: CheckpointBlockGossipService[F],
  config: Config,
  remoteCall: Blocker,
  calculationContext: ContextShift[F],
  metrics: Metrics,
  nodeId: Id
) {

  import ConsensusManager._

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]
  private val maxTransactionThreshold: Int =
    ConfigUtil.constellation.getInt("consensus.maxTransactionThreshold")
  private val maxObservationThreshold: Int =
    ConfigUtil.constellation.getInt("consensus.maxObservationThreshold")

  val timeout: Long =
    ConfigUtil.getDurationFromConfig("constellation.consensus.form-checkpoint-blocks-timeout").toMillis

  private val proposalsExpireTimeCache = ConfigUtil.getOrElse("constellation.cache.expire-after-min.cache", 10)

  private val semaphore: Semaphore[F] = createSemaphore()
  private[consensus] val consensuses: Ref[F, Map[RoundId, ConsensusInfo[F]]] = Ref.unsafe(
    Map.empty[RoundId, ConsensusInfo[F]]
  )
  private[consensus] val ownConsensus: Ref[F, Option[OwnConsensus[F]]] = Ref.unsafe(None)
  private[consensus] val proposals: StorageService[F, List[ConsensusProposal]] =
    new StorageService("ConsensusProposal".some, proposalsExpireTimeCache.minutes.some)

  private def withLock[R](name: String, thunk: F[R]) = new SingleLock[F, R](name, semaphore).use(thunk)

  def getRound(roundId: RoundId): F[Option[Consensus[F]]] =
    for {
      own <- ownConsensus.get
      maybe <- if (own.exists(_.roundId == roundId)) Sync[F].pure(own.flatMap(_.consensusInfo.map(_.consensus)))
      else
        consensuses.get.map(consensuses => consensuses.get(roundId).map(_.consensus))
    } yield maybe

  def getActiveMinHeight: F[Option[Long]] =
    consensuses.get.map(_.flatMap(_._2.tipMinHeight).toList match {
      case Nil  => None
      case list => Some(list.min)
    })

  def startOwnConsensus(): F[ConsensusInfo[F]] = {
    val startRoundTask = for {
      _ <- metrics.incrementMetricAsync("consensus_startOwnRound")
      roundId <- withLock("startOwnRound", syncRoundInProgress())
      _ <- logger.debug(s"[${nodeId.short}] Starting own consensus $roundId")
      roundData <- createRoundData(roundId)
      missing <- resolveMissingParents(roundData._1)
      roundInfo = ConsensusInfo[F](
        new Consensus[F](
          roundData._1,
          roundData._2,
          transactionService,
          checkpointService,
          messageService,
          observationService,
          remoteSender,
          this,
          apiClient,
          checkpointBlockGossipService,
          config,
          remoteCall,
          calculationContext
        ),
        roundData._1.tipsSOE.minHeight,
        System.currentTimeMillis()
      )
      _ <- ownConsensus.modify(d => (d.map(o => o.copy(consensusInfo = roundInfo.some)), ()))
      _ <- logger.debug(s"[${nodeId.short}] created data for round: ${roundId} with facilitators: ${roundData._1.peers
        .map(_.peerMetadata.id.short)}")
      responses <- remoteSender.notifyFacilitators(roundData._1)
      _ <- if (responses.forall(_ == true)) Sync[F].unit
      else
        Sync[F].raiseError[Unit](
          NotAllPeersParticipate(roundId, roundData._1.transactions, roundData._1.observations)
        )
    } yield roundInfo

    startRoundTask.recoverWith {
      case error: NoTipsForConsensus =>
        metrics
          .incrementMetricAsync("consensus_startOwnRound_noTipsForConsensusError") >>
          stopBlockCreationRound(StopBlockCreationRound(error.roundId, None, error.transactions, error.observations)) >>
          Sync[F].raiseError[ConsensusInfo[F]](error)
      case error: NoPeersForConsensus =>
        metrics.incrementMetricAsync("consensus_startOwnRound_noPeersForConsensusError") >>
          stopBlockCreationRound(StopBlockCreationRound(error.roundId, None, error.transactions, error.observations)) >>
          Sync[F].raiseError[ConsensusInfo[F]](error)
      case error: ConsensusStartError =>
        metrics.incrementMetricAsync("consensus_startOwnRound_consensusStartError") >>
          logger.debug(error.getMessage) >>
          Sync[F].raiseError[ConsensusInfo[F]](error)
      case error: ConsensusError =>
        metrics.incrementMetricAsync("consensus_startOwnRound_consensusError") >>
          logger.debug(error.getMessage) >>
          stopBlockCreationRound(StopBlockCreationRound(error.roundId, None, error.transactions, error.observations)) >>
          Sync[F].raiseError[ConsensusInfo[F]](error)
      case unknown =>
        metrics.incrementMetricAsync("consensus_startOwnRound_unknownError") >>
          logger.error(unknown)(s"Unexpected error when starting own consensus: ${unknown.getMessage}") >>
          forceStopOwnBlockCreationRound() >>
          Sync[F].raiseError[ConsensusInfo[F]](unknown)
    }
  }

  def syncRoundInProgress(): F[RoundId] =
    for {
      state <- cluster.getNodeState
      _ <- if (NodeState.canStartOwnConsensus(state)) Sync[F].unit
      else Sync[F].raiseError[Unit](InvalidNodeState(NodeState.validForOwnConsensus, state))
      own <- ownConsensus.get
      roundId <- if (own.isDefined) Sync[F].raiseError[RoundId](OwnRoundAlreadyInProgress)
      else
        ownConsensus.modify { _ =>
          val id = generateRoundId
          (Some(OwnConsensus(id, None)), id)
        }
    } yield roundId

  def createRoundData(roundId: RoundId): F[(RoundData, Seq[(ChannelMessage, Int)])] =
    for {
      transactions <- transactionService.pullForConsensus(maxTransactionThreshold)
      _ <- logger.info(s"Pulled for new consensus: ${transactions.size}")
      facilitators <- LiftIO[F].liftIO(readyFacilitatorsAsync)
      tips <- checkpointService.pullTips(facilitators)(metrics)
      _ <- if (tips.isEmpty)
        Sync[F].raiseError[Unit](NoTipsForConsensus(roundId, transactions.map(_.transaction), List.empty[Observation]))
      else Sync[F].unit
      _ <- if (tips.get.peers.isEmpty)
        Sync[F].raiseError[Unit](NoPeersForConsensus(roundId, transactions.map(_.transaction), List.empty[Observation]))
      else Sync[F].unit
      messages <- Seq() // Sync[F].delay(threadSafeMessageMemPool.pull().getOrElse(Seq()))
      observations <- observationService.pullForConsensus(maxObservationThreshold)
      lightNodes <- readyPeers(NodeType.Light)
      lightPeers = if (lightNodes.isEmpty) Set.empty[PeerData]
      else
        Set(lightNodes.minBy(p => Distance.calculate(transactions.head.transaction.baseHash, p._1))._2) // TODO: Choose more than one tx and light peers
      allFacilitators = tips.get.peers.values.map(_.peerMetadata.id).toSet ++ Set(nodeId)
      arbitraryMsgs <- getArbitraryMessagesWithDistance(allFacilitators).map(_.filter(t => t._2 == 1))
      roundData = (
        RoundData(
          roundId,
          tips.get.peers.values.toSet,
          lightPeers,
          FacilitatorId(nodeId),
          transactions.map(_.transaction),
          tips.get.tipSoe,
          messages,
          observations
        ),
        arbitraryMsgs
      )

    } yield roundData

  def participateInBlockCreationRound(roundData: RoundData): F[(ConsensusInfo[F], RoundData)] =
    (for {
      _ <- metrics.incrementMetricAsync("consensus_participateInRound")
      state <- cluster.getNodeState
      _ <- if (NodeState.canParticipateConsensus(state)) Sync[F].unit
      else Sync[F].raiseError[Unit](InvalidNodeState(NodeState.validForConsensusParticipation, state))
      allFacilitators = roundData.peers.map(_.peerMetadata.id) ++ Set(nodeId)
      arbitraryMsgs <- getArbitraryMessagesWithDistance(allFacilitators)
      updatedRoundData <- adjustPeers(roundData)
      roundInfo = ConsensusInfo(
        new Consensus[F](
          updatedRoundData,
          arbitraryMsgs,
          transactionService,
          checkpointService,
          messageService,
          observationService,
          remoteSender,
          this,
          apiClient,
          checkpointBlockGossipService,
          config,
          remoteCall,
          calculationContext
        ),
        roundData.tipsSOE.minHeight,
        System.currentTimeMillis()
      )
      _ <- consensuses.modify(r => (r + (roundData.roundId -> roundInfo), ()))
      _ <- logger.debug(s"[${nodeId.short}] Participate in round ${updatedRoundData.roundId}")
    } yield (roundInfo, updatedRoundData)).onError {
      case SnapshotHeightAboveTip(_, _, _) =>
        metrics.incrementMetricAsync("consensus_participateInRound_snapshotHeightAboveTipError")
      case InvalidNodeState(_, _) =>
        metrics.incrementMetricAsync("consensus_participateInRound_invalidNodeStateError")
      case _ => metrics.incrementMetricAsync("consensus_participateInRound_unknownError")
    }

  def continueRoundParticipation(roundInfo: ConsensusInfo[F], roundData: RoundData): F[Unit] =
    for {
      _ <- resolveMissingParents(roundData)
      _ <- withLock(roundData.roundId.toString, passMissed(roundData.roundId, roundInfo.consensus))
      _ <- roundInfo.consensus.startConsensusDataProposal()
    } yield ()

  def addMissed(roundId: RoundId, roundCommand: ConsensusProposal): F[Unit] =
    withLock(roundId.toString, addMissedUnsafe(roundId, roundCommand))

  private def addMissedUnsafe(roundId: RoundId, roundCommand: ConsensusProposal): F[Unit] =
    for {
      missed <- proposals.lookup(roundId.toString).map(_.toList.flatten)
      _ <- proposals.put(roundId.toString, missed :+ roundCommand)
      _ <- logger.debug(s"[${nodeId.short}] Put to proposals : ${roundId.toString}")
    } yield ()

  def passMissed(roundId: RoundId, consensus: Consensus[F]): F[Unit] =
    for {
      missed <- proposals.lookup(roundId.toString).map(_.toList.flatten)
      _ <- missed.traverse {
        case proposal: ConsensusDataProposal => consensus.addConsensusDataProposal(proposal)
        case proposal: SelectedUnionBlock    => consensus.addSelectedBlockProposal(proposal)
        case proposal: UnionBlockProposal    => consensus.addBlockProposal(proposal)
      }
    } yield ()

  def terminateConsensuses(): F[Unit] =
    for {
      _ <- logger.debug(
        s"[${nodeId.short}] Terminating all consensuses - waiting"
      )
      _ <- Timer[F].sleep(5 seconds)
      _ <- logger.debug(
        s"[${nodeId.short}] Terminating all consensuses"
      )
      runningConsensuses <- consensuses.get
      ownRound <- ownConsensus.get.map(_.flatMap(o => o.consensusInfo.map(i => o.roundId -> i)))
      toClean = (runningConsensuses ++ ownRound.toMap)
      _ <- cleanUpConsensuses(toClean)
    } yield ()

  def cleanUpConsensuses(consensuses: Map[RoundId, ConsensusInfo[F]]): F[Unit] =
    for {
      stopData <- consensuses.toList.traverse { r =>
        r._2.consensus.getOwnTransactionsToReturn
          .flatMap(txs => r._2.consensus.getOwnObservationsToReturn.map(exs => (r._1, txs, exs)))
      }
      _ <- if (stopData.nonEmpty)
        logger.warn(s"Cleaning consensuses with roundId: ${stopData.map(_._1)}")
      else
        Sync[F].unit
      _ <- stopData.traverse(s => stopBlockCreationRound(StopBlockCreationRound(s._1, None, s._2, s._3)))
    } yield ()

  def stopBlockCreationRound(cmd: StopBlockCreationRound): F[Unit] =
    for {
      _ <- consensuses.modify(curr => (curr - cmd.roundId, ()))
      _ <- ownConsensus.modify(
        curr => if (curr.isDefined && curr.get.roundId == cmd.roundId) (None, ()) else (curr, ())
      )
      _ <- transactionService.returnToPending(cmd.transactionsToReturn.map(_.hash))
      _ <- transactionService.clearInConsensus(cmd.transactionsToReturn.map(_.hash))
      _ <- observationService.returnToPending(cmd.observationsToReturn.map(_.hash))
      _ <- observationService.clearInConsensus(cmd.observationsToReturn.map(_.hash))
      _ <- updateNotifications(cmd.maybeCB.map(_.notifications.toList))
//      _ = releaseMessages(cmd.maybeCB)
      _ <- proposals.remove(cmd.roundId.toString)
      _ <- logger.debug(s"[${nodeId.short}] Removed from proposals : ${cmd.roundId.toString}")
      _ <- logger.debug(
        s"[${nodeId.short}] Consensus stopped ${cmd.roundId} with block: ${cmd.maybeCB.map(_.baseHash).getOrElse("empty")}"
      )
    } yield ()

  def forceStopOwnBlockCreationRound(): F[Unit] =
    for {
      _ <- ownConsensus.modify(_ => (None, ()))
      _ <- logger.info("Force stop - own block creation round")
    } yield ()

  def updateNotifications(notifications: Option[List[PeerNotification]]): F[Unit] =
    notifications match {
      case None      => Sync[F].unit
      case Some(Nil) => Sync[F].unit
      case Some(nonEmpty) =>
        cluster.updatePeerNotifications(nonEmpty)
    }

//  def releaseMessages(maybeCB: Option[CheckpointBlock]): Unit =
//    maybeCB.foreach(
//      cb =>
//        cb.messages.foreach(
//          message =>
//            threadSafeMessageMemPool.activeChannels
//              .get(message.signedMessageData.data.channelId)
//              .foreach(_.release())
//        )
//    )

  def cleanUpLongRunningConsensus: F[Unit] =
    for {
      runningConsensuses <- consensuses.get
      currentTime <- Sync[F].delay(System.currentTimeMillis())
      ownRound <- ownConsensus.get.map(_.flatMap(o => o.consensusInfo.map(i => o.roundId -> i)))
      toClean = (runningConsensuses ++ ownRound.toMap).filter(r => (currentTime - r._2.startTime) > timeout)
      _ <- cleanUpConsensuses(toClean)
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
          .filter(_.peerMetadata.id != nodeId) + initiator
      )
    }

  private[consensus] def resolveMissingParents(
    roundData: RoundData
  ): F[List[CheckpointCache]] =
    for {
      soes <- roundData.tipsSOE.soe.toList.pure[F]
      peers = roundData.peers.map(_.peerMetadata.toPeerClientMetadata)
      existing <- soes.map(_.hash).traverse(checkpointService.lookupSoe).map(_.flatten)
      missing = soes.diff(existing)

      resolved <- missing
        .map(_.baseHash)
        .filterA(checkpointService.containsCheckpoint(_).map(!_))
        .flatTap { hashes =>
          logger.debug(s"${roundData.roundId}] Trying to resolve: ${hashes}")
        }
        .flatMap {
          _.traverse { hash =>
            dataResolver.resolveCheckpointDefaults(hash, peers.find(_.id == roundData.facilitatorId.id))
          }
        }
      _ <- logger.debug(
        s"[${nodeId.short}] Missing parents size=${missing.size}, existing size=${existing.size}, resolved size=${resolved.size} for round ${roundData.roundId}"
      )
      _ <- if (missing.nonEmpty && (resolved.size != missing.size))
        logger.error(s"Missing parents: ${missing.map(_.hash)} with base hashes: ${missing.map(_.baseHash)}")
      else Sync[F].unit
    } yield resolved

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
            .indexOf(nodeId)
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

  case class BroadcastConsensusDataProposal(
    roundId: RoundId,
    peers: Set[PeerData],
    consensusDataProposal: ConsensusDataProposal
  )

  case object OwnRoundAlreadyInProgress extends ConsensusStartError("Node has already start own consensus")

  class ConsensusStartError(message: String) extends Exception(message)

  class ConsensusError(
    val roundId: RoundId,
    val transactions: List[Transaction],
    val observations: List[Observation],
    message: String
  ) extends Exception(message)

  case class NoTipsForConsensus(id: RoundId, txs: List[Transaction], obs: List[Observation])
      extends ConsensusError(id, txs, obs, s"No tips to start consensus $id")
  case class NoPeersForConsensus(id: RoundId, txs: List[Transaction], obs: List[Observation])
      extends ConsensusError(id, txs, obs, s"No active peers to start consensus $id")
  case class NotAllPeersParticipate(id: RoundId, txs: List[Transaction], obs: List[Observation])
      extends ConsensusError(id, txs, obs, s"Not all of the peers has participated in consensus $id")

  case class BroadcastUnionBlockProposal(roundId: RoundId, peers: Set[PeerData], proposal: UnionBlockProposal)
  case class BroadcastSelectedUnionBlock(roundId: RoundId, peers: Set[PeerData], cb: SelectedUnionBlock)
  case class ConsensusTimeout(roundId: RoundId)

  case class SnapshotHeightAboveTip(id: RoundId, snapHeight: Long, tipHeight: Long)
      extends Exception(
        s"Can't participate in round $id snapshot height: $snapHeight is above or/equal proposed tip $tipHeight"
      )
}
