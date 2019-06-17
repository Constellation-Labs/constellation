package org.constellation.consensus

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Cancellable, Props}
import cats.implicits._
import io.micrometer.core.instrument.Timer
import org.constellation.consensus.CrossTalkConsensus.{NotifyFacilitators, ParticipateInBlockCreationRound, StartNewBlockCreationRound}
import org.constellation.consensus.Round._
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCache, Id, NodeType}
import org.constellation.primitives.{PeerData, UpdatePeerNotifications, _}
import org.constellation.util.{Distance, PeerApiClient}
import org.constellation.{ConfigUtil, DAO}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

class RoundManager(roundTimeout: FiniteDuration)(implicit dao: DAO)
    extends Actor
    with ActorLogging {
  import RoundManager._

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  private[consensus] var ownRoundInProgress: Boolean = false

  private[consensus] val rounds: mutable.Map[RoundId, RoundInfo] =
    mutable.Map[RoundId, RoundInfo]()

  override def receive: Receive = {
    case StartNewBlockCreationRound if ownRoundInProgress =>
      log.info(
        s"Unable to initiate new round another round: ${rounds.filter(_._2.startedByThisNode)} is in progress"
      )

    case StartNewBlockCreationRound if !ownRoundInProgress =>
      ownRoundInProgress = true
      createRoundData(dao).fold {
        log.error("Cannot create a round data")
        ownRoundInProgress = false
      } { tuple =>
        val roundData = tuple._1
        log.debug(
          s"node: ${dao.id.short} starting new round ${roundData.roundId} with facilis: ${roundData.peers
            .map(_.peerMetadata.id.short)}"
        )
        resolveMissingParents(roundData).onComplete {
          case Failure(e) =>
            ownRoundInProgress = false
            log.error(e, s"unable to start block creation round due to: ${e.getMessage}")
          case Success(_) =>
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
            log.debug(s"node: ${dao.id.short} starting new round: ${roundData.roundId}")
            dao.blockFormationInProgress = true
            dao.metrics.updateMetric("blockFormationInProgress", dao.blockFormationInProgress.toString)
        }
      }

    case cmd: ParticipateInBlockCreationRound =>
      log.info(s"node: ${dao.id.short} participating in round: ${cmd.roundData.roundId}")
      resolveMissingParents(cmd.roundData).onComplete {
        case Failure(e) =>
          log.error(
            e,
            s"unable to participate in block creation round: ${cmd.roundData.roundId} due to: ${e.getMessage}"
          )
        case Success(_) =>
          val allFacilitators = cmd.roundData.peers.map(_.peerMetadata.id) ++ Set(dao.id)
          startRound(adjustPeers(cmd.roundData),
                     getArbitraryTransactionsWithDistance(allFacilitators, dao),
                     getArbitraryMessagesWithDistance(allFacilitators, dao))
          passToRoundActor(StartTransactionProposal(cmd.roundData.roundId))
      }

    case cmd: LightTransactionsProposal =>
      passToRoundActor(cmd)

    case cmd: ConsensusTimeout =>
      log.error(s"Consensus with roundId: ${cmd.roundId} timeout on node: ${dao.id.short}")
      self ! StopBlockCreationRound(cmd.roundId, None)
    case cmd: StopBlockCreationRound =>
      rounds.get(cmd.roundId).fold {} { round =>
        dao.metrics.stopTimer("crosstalkConsensus", round.timer)
        log.info(s"Stop block creation round has been triggered for round ${cmd.roundId} on node ${dao.id.short}")

        round.timeoutScheduler.cancel()
        if (round.startedByThisNode) {
          ownRoundInProgress = false
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

    case msg => log.info(s"Received unknown message: $msg")

  }

  private[consensus] def closeRoundActor(roundId: RoundId): Unit = {
    rounds
      .get(roundId)
      .foreach(r => context.stop(r.roundActor))
  }

  private[consensus] def adjustPeers(roundData: RoundData): RoundData = {
    val updatedPeers = roundData.peers
      .filter(_.peerMetadata.id != dao.id)
      .union(Set(dao.peerInfoAsync.unsafeRunSync().get(roundData.facilitatorId.id) match {
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
  )(implicit dao: DAO): Future[List[Option[CheckpointCache]]] = {

    implicit val ec = dao.edgeExecutionContext

    val cbToResolve = roundData.tipsSOE
      .filterNot(t => dao.checkpointService.contains(t.baseHash))
      .map(_.baseHash)

    DataResolver
      .resolveCheckpoints(
        cbToResolve.toList,
        roundData.peers.map(r => PeerApiClient(r.peerMetadata.id, r.client)),
        dao.peerInfoAsync.unsafeRunSync()
          .get(roundData.facilitatorId.id)
          .map(x => PeerApiClient(roundData.facilitatorId.id, x.client))
      )
      .unsafeToFuture()
  }

  private[consensus] def startRound(roundData: RoundData,
                                    arbitraryTransactions: Seq[(Transaction, Int)],
                                    arbitraryMessages: Seq[(ChannelMessage, Int)],
                                    startedByThisNode: Boolean = false): Unit = {
    rounds += roundData.roundId -> RoundInfo(
      generateRoundActor(roundData, arbitraryTransactions, arbitraryMessages, dao),
      context.system.scheduler.scheduleOnce(roundTimeout,
                                            self,
        ConsensusTimeout(roundData.roundId)
      ),
      startedByThisNode,
      dao.metrics.startTimer
    )
    log.info(s"Started round=${roundData.roundId}")
  }

  private[consensus] def passToRoundActor(cmd: RoundCommand): Unit = {
    rounds.get(cmd.roundId).foreach(_.roundActor ! cmd)
  }

  private[consensus] def passToParentActor(cmd: Any): Unit = {
    context.parent ! cmd
  }
}

object RoundManager {
  def props(roundTimeout: FiniteDuration)(implicit dao: DAO): Props =
    Props(new RoundManager(roundTimeout))

  def generateRoundActor(roundData: RoundData,
                         arbitraryTransactions: Seq[(Transaction, Int)],
                         arbitraryMessages: Seq[(ChannelMessage, Int)],
                         dao: DAO)(implicit context: ActorContext): ActorRef =
    context.actorOf(
      Round.props(roundData, arbitraryTransactions, arbitraryMessages, dao, DataResolver)
    )

  def createRoundData(
    dao: DAO
  ): Option[(RoundData, Seq[(Transaction, Int)], Seq[(ChannelMessage, Int)])] = {
    dao
      .pullTips(dao.readyFacilitatorsAsync.unsafeRunSync())
      .map(tips => {
        val transactions = dao.transactionService
          .pullForConsensus(dao.minCheckpointFormationThreshold)
          .unsafeRunSync()
        val messages = dao.threadSafeMessageMemPool.pull().getOrElse(Seq()) // TODO: Choose more than one tx and light peers
        val firstTx = transactions.headOption
        val lightPeers = if (firstTx.isDefined && dao.readyPeersAsync(NodeType.Light).unsafeRunSync().nonEmpty) {
          Set(
            dao
              .readyPeersAsync(NodeType.Light)
              .unsafeRunSync()
              .minBy(p => Distance.calculate(firstTx.get.transaction.baseHash, p._1))
              ._2
          )
        } else Set[PeerData]()
        val allFacilitators = tips._2.values.map(_.peerMetadata.id).toSet ++ Set(dao.id)

        (RoundData(
           generateRoundId,
           tips._2.values.toSet,
           lightPeers,
           FacilitatorId(dao.id),
           transactions.map(_.transaction),
           tips._1,
           messages
         ),
         getArbitraryTransactionsWithDistance(allFacilitators, dao).filter(t => t._2 == 1),
         getArbitraryMessagesWithDistance(allFacilitators, dao).filter(t => t._2 == 1))
      })
  }

  def getArbitraryTransactionsWithDistance(facilitators: Set[Id],
                                           dao: DAO): Seq[(Transaction, Int)] = {

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

    dao.transactionService
      .getArbitrary
      .unsafeRunSync()
      .map { t =>
        (t._2.transaction,
         facilitators
           .map(f => (f, measureDistance(f, t._2.transaction)))
           .toSeq
           .sortBy(_._2)
           .map(_._1)
           .indexOf(dao.id))
      }
      .toSeq
  }

  def getArbitraryMessagesWithDistance(facilitators: Set[Id],
                                       dao: DAO): Seq[(ChannelMessage, Int)] = {

    def measureDistance(id: Id, tx: ChannelMessage) = {
      BigInt(id.hex.getBytes()) ^ BigInt(tx.signedMessageData.hash.getBytes)
    }

    dao.messageService.arbitraryPool
      .toMapSync()
      .map { m =>
        (m._2.channelMessage,
         facilitators
           .map(f => (f, measureDistance(f, m._2.channelMessage)))
           .toSeq
           .sortBy(_._2)
           .map(_._1)
           .indexOf(dao.id))
      }
      .toSeq
  }

  def generateRoundId: RoundId =
    RoundId(java.util.UUID.randomUUID().toString)

  case class RoundInfo(
    roundActor: ActorRef,
    timeoutScheduler: Cancellable,
    startedByThisNode: Boolean = false,
    timer: Timer.Sample
  )

  case class BroadcastLightTransactionProposal(
    peers: Set[PeerData],
    transactionsProposal: LightTransactionsProposal
  )

  case class BroadcastUnionBlockProposal(peers: Set[PeerData], proposal: UnionBlockProposal)

  case class BroadcastSelectedUnionBlock(peers: Set[PeerData], cb: SelectedUnionBlock)

}
