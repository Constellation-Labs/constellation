package org.constellation.consensus

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Cancellable, Props}
import cats.effect.IO
import constellation.wrapFutureWithMetric
import org.constellation.consensus.CrossTalkConsensus.{NotifyFacilitators, ParticipateInBlockCreationRound, StartNewBlockCreationRound}
import org.constellation.consensus.Round._
import org.constellation.p2p.DataResolver
import org.constellation.primitives.Schema.{CheckpointCacheData, NodeType, SignedObservationEdge}
import org.constellation.primitives.{PeerData, UpdatePeerNotifications}
import org.constellation.util.{Distance, PeerApiClient}
import org.constellation.{ConfigUtil, DAO}
import cats.implicits._

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

class RoundManager(implicit dao: DAO) extends Actor with ActorLogging {
  import RoundManager._

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val roundTimeout: FiniteDuration = ConfigUtil.getDurationFromConfig(
    "constellation.consensus.form-checkpoint-blocks-timeout",
    60.second
  )
  val rounds: mutable.Map[RoundId, RoundInfo] =
    mutable.Map[RoundId, RoundInfo]()

  override def receive: Receive = {
    case StartNewBlockCreationRound if rounds.exists(_._2.startedByThisNode) =>
      log.debug(
        s"Unable to initiate new round another round: ${rounds.filter(_._2.startedByThisNode)} is in progress"
      )

    case StartNewBlockCreationRound if !rounds.exists(_._2.startedByThisNode) =>
      createRoundData(dao).foreach { roundData =>
        resolveMissingParents(roundData).onComplete {
          case Failure(e) =>
            log.error(e, s"unable to start block creation round due to: ${e.getMessage}")
          case Success(_) =>
            startRound(roundData, startedByThisNode = true)
            context.parent ! NotifyFacilitators(roundData)
            passToRoundActor(
              LightTransactionsProposal(roundData.roundId,
                                   FacilitatorId(dao.id),
                                   roundData.transactions.map(_.hash),
                                   roundData.messages.map(_.signedMessageData.hash),
                                   roundData.peers.flatMap(_.notification).toSeq)
            )
            log.debug(s"node: ${dao.id.short} starting new round: ${roundData.roundId}")
            dao.blockFormationInProgress = true
        }
      }

    case cmd: ParticipateInBlockCreationRound =>
      log.debug(s"node: ${dao.id.short} participating in round: ${cmd.roundData.roundId}")
      resolveMissingParents(cmd.roundData).onComplete {
        case Failure(e) =>
          log.error(e, s"unable to start block creation round due to: ${e.getMessage}")
          // TODO: should I interrupt others and abort the whole Round ?
        case Success(_) =>
          startRound(adjustPeers(cmd.roundData))
          passToRoundActor(StartTransactionProposal(cmd.roundData.roundId))
      }

    case cmd: LightTransactionsProposal =>
      passToRoundActor(cmd)

    case cmd: StopBlockCreationRound =>
      rounds.get(cmd.roundId).foreach(_.timeoutScheduler.cancel())
      rounds.remove(cmd.roundId)

      cmd.maybeCB.foreach(cb =>
        cb.messages.foreach(message =>
          dao.threadSafeMessageMemPool.activeChannels
            .get(message.signedMessageData.data.channelId)
            .foreach(_.release()))
      )

      cmd.maybeCB.foreach(cb => dao.peerManager ! UpdatePeerNotifications(cb.notifications))

      dao.blockFormationInProgress = false

    case cmd: BroadcastLightTransactionProposal =>
      passToParentActor(cmd)

    case cmd: BroadcastUnionBlockProposal =>
      passToParentActor(cmd)

    case cmd: UnionBlockProposal =>
      passToRoundActor(cmd)

    case cmd: ResolveMajorityCheckpointBlock =>
      log.debug(
        s"node ${dao.id.short} Block formation timeout occurred for ${cmd.roundId} resolving majority CheckpointBlock"
      )
      passToRoundActor(cmd)

    case msg => log.info(s"Received unknown message: $msg")

  }

  def adjustPeers(roundData: RoundData): RoundData = {
    val updatedPeers = roundData.peers
      .filter(_.peerMetadata.id != dao.id)
      .union(Set(dao.peerInfo.get(roundData.facilitatorId.id) match {
        case Some(value) => value
        case None =>
          throw new IllegalStateException(
            s"Unable to find round initiator for round ${roundData.roundId} and facilitatorId: ${roundData.facilitatorId}"
          )
      }))

    roundData.copy(peers = updatedPeers)
  }

  def resolveMissingParents(
    roundData: RoundData
  )(implicit dao: DAO): Future[List[Option[CheckpointCacheData]]] = {

    implicit val ec = dao.edgeExecutionContext

    val cbToResolve = roundData.tipsSOE
      .filterNot(t => dao.checkpointService.contains(t.baseHash))
      .map(_.baseHash)

    DataResolver.resolveCheckpoints(cbToResolve.toList,roundData.peers.map(r => PeerApiClient(r.peerMetadata.id, r.client)))
      .unsafeToFuture()
  }

  def startRound(roundData: RoundData, startedByThisNode: Boolean = false): Unit = {
    rounds += roundData.roundId -> RoundInfo(
      generateRoundActor(roundData, dao),
      context.system.scheduler
        .scheduleOnce(
          roundTimeout,
          self,
          ResolveMajorityCheckpointBlock(roundData.roundId)
        ),
      startedByThisNode
    )
  }

  def passToRoundActor(cmd: RoundCommand): Unit = {
    rounds.get(cmd.roundId).foreach(_.roundActor ! cmd)
  }

  def passToParentActor(cmd: Any): Unit = {
    context.parent ! cmd
  }
}

object RoundManager {
  def props(implicit dao: DAO): Props = Props(new RoundManager)

  case class RoundInfo(roundActor: ActorRef,
                       timeoutScheduler: Cancellable,
                       startedByThisNode: Boolean = false)

  case class BroadcastLightTransactionProposal(
    peers: Set[PeerData],
    transactionsProposal: LightTransactionsProposal
  )

  case class BroadcastUnionBlockProposal(peers: Set[PeerData], proposal: UnionBlockProposal)

  def generateRoundId: RoundId =
    RoundId(java.util.UUID.randomUUID().toString)

  def generateRoundActor(roundData: RoundData, dao: DAO)(implicit context: ActorContext): ActorRef =
    context.actorOf(Round.props(roundData, dao))

  def createRoundData(dao: DAO): Option[RoundData] = {
    dao.pullTransactions(dao.minCheckpointFormationThreshold)
      .flatMap { transactions => dao.pullTips(dao.readyFacilitators()).map(tips => {
        val messages = dao.threadSafeMessageMemPool.pull().getOrElse(Seq())

        // TODO: Choose more than one tx and light peers
        val firstTx = transactions.head
        val lightPeers = if (dao.readyPeers(NodeType.Light).nonEmpty) {
          Set(
            dao.readyPeers(NodeType.Light).minBy(p => Distance.calculate(firstTx.baseHash, p._1))._2
          )
        } else Set[PeerData]()

        RoundData(
          generateRoundId,
          tips._2.values.toSet,
          lightPeers,
          FacilitatorId(dao.id),
          transactions,
          tips._1,
          messages
        )
      }) }
  }
}
