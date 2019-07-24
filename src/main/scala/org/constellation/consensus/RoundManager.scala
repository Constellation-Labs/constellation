package org.constellation.consensus

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{Actor, ActorContext, ActorRef, Cancellable, OneForOneStrategy, Props}
import cats.effect.{ContextShift, IO}
import cats.effect.IO
import cats.effect.IO.RaiseError
import cats.implicits._
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging
import io.micrometer.core.instrument.Timer
import org.constellation.consensus.CrossTalkConsensus.{
  NotifyFacilitators,
  ParticipateInBlockCreationRound,
  StartNewBlockCreationRound
}
import org.constellation.consensus.Round._
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCache, Height, Id, NodeState, NodeType}
import org.constellation.primitives.{PeerData, UpdatePeerNotifications, _}
import org.constellation.storage.StorageService
import org.constellation.util.{Distance, PeerApiClient}
import org.constellation.{ConfigUtil, ConstellationContextShift, ConstellationExecutionContext, DAO}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class RoundManager(config: Config)(implicit dao: DAO) extends Actor with StrictLogging {
  import RoundManager._

  implicit val contextShift: ContextShift[IO] = ConstellationContextShift.edge

  override val supervisorStrategy: OneForOneStrategy = {

    import akka.actor.SupervisorStrategy.Stop
    OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 1 minute) {
      case _: Exception => Stop
    }
  }
  private[consensus] val rounds: mutable.Map[RoundId, RoundInfo] =
    mutable.Map[RoundId, RoundInfo]()

  private val messagesWithoutRound =
    new StorageService[IO, Seq[RoundCommand]](expireAfterMinutes = Some(2))
  private[consensus] var ownRoundInProgress: AtomicBoolean = new AtomicBoolean(false)

  override def receive: Receive = {
    case GetActiveMinHeight =>
      val replyTo = sender()
      val minHeight = rounds.flatMap(_._2.tipMinHeight).toList match {
        case Nil  => None
        case list => Some(list.min)
      }
      replyTo ! ActiveTipMinHeight(minHeight)
    case StartNewBlockCreationRound if ownRoundInProgress.get() =>
      logger.warn(
        s"Unable to initiate new round another round: ${rounds.filter(_._2.startedByThisNode)} is in progress"
      )
    case StartNewBlockCreationRound if dao.nodeState != NodeState.Ready =>
      logger.warn(
        s"Unable to initiate new round node not ready: ${dao.nodeState}"
      )
    case StartNewBlockCreationRound if !ownRoundInProgress.get() && dao.nodeState == NodeState.Ready =>
      ownRoundInProgress.compareAndSet(false, true)
      createRoundData(dao).onComplete {
        case Failure(e) =>
          logger.debug("Cannot create a round data due to no transactions")
          ownRoundInProgress.compareAndSet(true, false)
        case Success(Some(tuple)) =>
          val roundData = tuple._1
          logger.debug(
            s"node: ${dao.id.short} starting new round ${roundData.roundId} with facilis: ${roundData.peers
              .map(_.peerMetadata.id.short)}"
          )
          resolveMissingParents(roundData).onComplete {
            case Failure(e) =>
              ownRoundInProgress.compareAndSet(true, false)
              logger.error(s"unable to start block creation round due to: ${e.getMessage}", e)
            case Success(_) =>
              logger.debug(s"[${dao.id.short}] ${roundData.roundId} started round")
              startRound(roundData, tuple._2, tuple._3, startedByThisNode = true)
              passToParentActor(NotifyFacilitators(roundData))
              passToRoundActor(
                LightTransactionsProposal(
                  roundData.roundId,
                  FacilitatorId(dao.id),
                  roundData.transactions.map(_.hash) ++ tuple._2.filter(_._2 == 0).map(_._1.hash),
                  roundData.messages.map(_.signedMessageData.hash),
                  roundData.peers.flatMap(_.notification).toSeq
                )
              )
              logger.debug(s"node: ${dao.id.short} starting new round: ${roundData.roundId}")
              dao.blockFormationInProgress = true
              dao.metrics.updateMetric("blockFormationInProgress", dao.blockFormationInProgress.toString)
          }(ConstellationExecutionContext.global)
      }(ConstellationExecutionContext.global)

    case _: ParticipateInBlockCreationRound if dao.nodeState != NodeState.Ready =>
      logger.warn(
        s"Unable to initiate new round node not ready: ${dao.nodeState}"
      )
    case cmd: ParticipateInBlockCreationRound if dao.nodeState == NodeState.Ready =>
      val checkHeight = cmd.roundData.tipsSOE.minHeight.traverse { min =>
        dao.snapshotService.lastSnapshotHeight.get
          .flatMap(last => if (last >= min) IO.raiseError[Unit](SnapshotHeightAboveTip(last, min)) else IO.unit)
      }
      checkHeight.unsafeRunSync()

      resolveMissingParents(cmd.roundData).onComplete {
        case Failure(e) =>
          logger.error(
            s"unable to participate in block creation round: ${cmd.roundData.roundId} due to: ${e.getMessage}",
            e
          )
        case Success(_) =>
          val allFacilitators = cmd.roundData.peers.map(_.peerMetadata.id) ++ Set(dao.id)
          startRound(
            adjustPeers(cmd.roundData),
            getArbitraryTransactionsWithDistance(allFacilitators, dao),
            getArbitraryMessagesWithDistance(allFacilitators, dao)
          )
          passToRoundActor(StartTransactionProposal(cmd.roundData.roundId))
          messagesWithoutRound.lookup(cmd.roundData.roundId.id).unsafeRunSync().foreach { commands =>
            commands.foreach(passToRoundActor)
          }
      }(ConstellationExecutionContext.global)

    case cmd: LightTransactionsProposal =>
      passToRoundActor(cmd)

    case cmd: ConsensusTimeout =>
      logger.error(s"Consensus with roundId: ${cmd.roundId} timeout on node: ${dao.id.short}")
      self ! StopBlockCreationRound(cmd.roundId, None, Seq.empty)
    case cmd: RoundException =>
      logger.error(s"Consensus on node: ${dao.id.short} finished with error {}", cmd)
      self ! StopBlockCreationRound(cmd.roundId, None, cmd.transactionsToReturn)

    case cmd: StopBlockCreationRound =>
      rounds.get(cmd.roundId).fold {} { round =>
        dao.metrics.stopTimer("crosstalkConsensus", round.timer)
        logger.debug(
          s"Stop block creation round has been triggered for round ${cmd.roundId} on node ${dao.id.short}"
        )

        round.timeoutScheduler.cancel()
        if (round.startedByThisNode) {
          ownRoundInProgress.compareAndSet(true, false)
        }
      }
      closeRoundActor(cmd.roundId)
      rounds.remove(cmd.roundId)

      cmd.maybeCB.foreach(
        cb =>
          cb.messages.foreach(
            message =>
              dao.threadSafeMessageMemPool.activeChannels
                .get(message.signedMessageData.data.channelId)
                .foreach(_.release())
        )
      )
      returnTransactionsToPending(cmd.transactionsToReturn, cmd.roundId)
      cmd.maybeCB.foreach(cb => dao.peerManager ! UpdatePeerNotifications(cb.notifications))

      dao.blockFormationInProgress = false
      dao.metrics.updateMetric("blockFormationInProgress", dao.blockFormationInProgress.toString)

    case cmd: BroadcastLightTransactionProposal =>
      passToParentActor(cmd)

    case cmd: BroadcastUnionBlockProposal =>
      passToParentActor(cmd)

    case cmd: UnionBlockProposal =>
      passToRoundActor(cmd)

    case cmd: BroadcastSelectedUnionBlock =>
      passToParentActor(cmd)

    case cmd: SelectedUnionBlock =>
      passToRoundActor(cmd)

    case msg => logger.warn(s"Received unknown message: $msg")

  }

  private[consensus] def returnTransactionsToPending(transactionsToReturn: Seq[String], roundId: RoundId): Unit =
    dao.transactionService.returnTransactionsToPending(transactionsToReturn).unsafeRunAsync {
      case Right(Nil) => ()
      case Right(txs) =>
        logger.debug(
          s"Transactions returned to pending state ${txs.map(_.transaction.hash)} in round $roundId"
        )
      case Left(value) =>
        logger.error(
          s"Unable to cleanup transactions $transactionsToReturn from consensus round $roundId {}",
          value
        )
    }

  private[consensus] def closeRoundActor(roundId: RoundId): Unit =
    rounds
      .get(roundId)
      .foreach(r => context.stop(r.roundActor))

  private[consensus] def adjustPeers(roundData: RoundData): RoundData = {
    val updatedPeers = roundData.peers
      .filter(_.peerMetadata.id != dao.id)
      .union(Set(dao.peerInfo.unsafeRunSync().get(roundData.facilitatorId.id) match {
        case Some(value) => value
        case None =>
          throw new IllegalStateException(
            s"Unable to find round initiator for round ${roundData.roundId} and facilitatorId: ${roundData.facilitatorId}"
          )
      }))

    roundData.copy(peers = updatedPeers)
  }

  private[consensus] def resolveMissingParents(
    roundData: RoundData
  )(implicit dao: DAO): Future[List[CheckpointCache]] =
    roundData.tipsSOE.soe
      .filterNot(t => dao.checkpointService.contains(t.baseHash).unsafeRunSync())
      .map(_.baseHash) match {
      case Nil => Future.successful(List.empty)
      case nel =>
        dao.readyPeers
          .map(_.mapValues(p => PeerApiClient(p.peerMetadata.id, p.client)))
          .flatMap(
            peers =>
              DataResolver
                .resolveCheckpoints(nel.toList, peers.values.toList, peers.get(roundData.facilitatorId.id))
          )
          .unsafeToFuture()
    }

  private[consensus] def startRound(
    roundData: RoundData,
    arbitraryTransactions: Seq[(Transaction, Int)],
    arbitraryMessages: Seq[(ChannelMessage, Int)],
    startedByThisNode: Boolean = false
  ): Unit = {
    rounds += roundData.roundId -> RoundInfo(
      generateRoundActor(roundData, arbitraryTransactions, arbitraryMessages, dao, config),
      context.system.scheduler.scheduleOnce(
        ConfigUtil.getDurationFromConfig("constellation.consensus.form-checkpoint-blocks-timeout", 60 seconds, config),
        self,
        ConsensusTimeout(roundData.roundId)
      )(ConstellationExecutionContext.apiClient),
      startedByThisNode,
      roundData.tipsSOE.minHeight,
      dao.metrics.startTimer
    )
    logger.debug(s"Started round=${roundData.roundId}")
  }

  private[consensus] def passToRoundActor(cmd: RoundCommand): Unit =
    rounds.get(cmd.roundId) match {
      case Some(info) => info.roundActor ! cmd
      case None =>
        messagesWithoutRound
          .update(cmd.roundId.id, { commands =>
            commands :+ cmd
          }, {
            Seq(cmd)
          })
          .unsafeRunSync()
    }

  private[consensus] def passToParentActor(cmd: Any): Unit =
    context.parent ! cmd
}

object RoundManager {

  def props(config: Config)(implicit dao: DAO): Props =
    Props(new RoundManager(config))

  def generateRoundActor(
    roundData: RoundData,
    arbitraryTransactions: Seq[(Transaction, Int)],
    arbitraryMessages: Seq[(ChannelMessage, Int)],
    dao: DAO,
    config: Config
  )(implicit context: ActorContext): ActorRef =
    context.actorOf(
      Round.props(roundData, arbitraryTransactions, arbitraryMessages, dao, DataResolver, config)
    )

  def createRoundData(
    dao: DAO
  ): Future[Option[(RoundData, Seq[(Transaction, Int)], Seq[(ChannelMessage, Int)])]] = {

    val task = for {
      transactions <- dao.transactionService.pullForConsensus(dao.minCheckpointFormationThreshold)
      _ <- if (transactions.isEmpty) NoTransactionsForConsensus.raiseError[IO, Unit] else IO.unit
      facilitators <- dao.readyFacilitatorsAsync
      tips = dao.pullTips(facilitators)
      _ <- if (tips.isEmpty) IO.raiseError[Unit](NoTransactionsForConsensus) else IO.unit
      messages = dao.threadSafeMessageMemPool.pull().getOrElse(Seq())
      lightNodes <- dao.readyPeers(NodeType.Light)
      lightPeers = if (lightNodes.isEmpty) Set.empty[PeerData]
      else
        Set(lightNodes.minBy(p => Distance.calculate(transactions.head.transaction.baseHash, p._1))._2) // TODO: Choose more than one tx and light peers
      allFacilitators = tips.get.peers.values.map(_.peerMetadata.id).toSet ++ Set(dao.id)
      roundData = (
        RoundData(
          generateRoundId,
          tips.get.peers.values.toSet,
          lightPeers,
          FacilitatorId(dao.id),
          transactions.map(_.transaction),
          tips.get.tipSoe,
          messages
        ),
        getArbitraryTransactionsWithDistance(allFacilitators, dao).filter(t => t._2 == 1),
        getArbitraryMessagesWithDistance(allFacilitators, dao).filter(t => t._2 == 1)
      )

    } yield roundData.some

    task.handleError {
      case NoTransactionsForConsensus =>
        None
    }
    task.unsafeToFuture()
  }

  def getArbitraryTransactionsWithDistance(facilitators: Set[Id], dao: DAO): Seq[(Transaction, Int)] = {

    val measureDistance =
      Try(
        ConfigUtil.config.getString("constellation.consensus.arbitrary-tx-distance-base")
      ).getOrElse("hash") match {
        case "id" =>
          (id: Id, tx: Transaction) =>
            Distance.calculate(tx.src.address, id)
        case "hash" =>
          (id: Id, tx: Transaction) =>
            val idBi = BigInt(id.hex.getBytes())
            val txBi = BigInt(tx.hash.getBytes())
            val srcBi = BigInt(tx.src.address.getBytes())
            (idBi ^ txBi) + (idBi ^ srcBi)
      }

    dao.transactionService.getArbitrary
      .unsafeRunSync()
      .map { t =>
        (
          t._2.transaction,
          facilitators
            .map(f => (f, measureDistance(f, t._2.transaction)))
            .toSeq
            .sortBy(_._2)
            .map(_._1)
            .indexOf(dao.id)
        )
      }
      .toSeq
  }

  def getArbitraryMessagesWithDistance(facilitators: Set[Id], dao: DAO): Seq[(ChannelMessage, Int)] = {

    def measureDistance(id: Id, tx: ChannelMessage) =
      BigInt(id.hex.getBytes()) ^ BigInt(tx.signedMessageData.hash.getBytes)

    dao.messageService.arbitraryPool
      .toMap()
      .unsafeRunSync()
      .map { m =>
        (
          m._2.channelMessage,
          facilitators
            .map(f => (f, measureDistance(f, m._2.channelMessage)))
            .toSeq
            .sortBy(_._2)
            .map(_._1)
            .indexOf(dao.id)
        )
      }
      .toSeq
  }

  def generateRoundId: RoundId =
    RoundId(java.util.UUID.randomUUID().toString)

  case class RoundInfo(
    roundActor: ActorRef,
    timeoutScheduler: Cancellable,
    startedByThisNode: Boolean = false,
    tipMinHeight: Option[Long],
    timer: Timer.Sample
  )

  case class BroadcastLightTransactionProposal(
    roundId: RoundId,
    peers: Set[PeerData],
    transactionsProposal: LightTransactionsProposal
  )

  case object NoTransactionsForConsensus extends Exception
  case class BroadcastUnionBlockProposal(roundId: RoundId, peers: Set[PeerData], proposal: UnionBlockProposal)

  case class BroadcastSelectedUnionBlock(roundId: RoundId, peers: Set[PeerData], cb: SelectedUnionBlock)

  case class ConsensusTimeout(roundId: RoundId)

  case object GetActiveMinHeight
  case class GetActiveMinHeight(replyTo: ActorRef)
  case class ActiveTipMinHeight(minHeight: Option[Long])
  case class SnapshotHeightAboveTip(snapHeight: Long, tipHeight: Long)
      extends Exception(s"Snapshot height: $snapHeight is above or/equal proposed tip $tipHeight")
}
