package org.constellation.consensus

import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import cats.effect.IO
import cats.implicits._
import constellation.{wrapFutureWithMetric, _}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{BroadcastLightTransactionProposal, BroadcastUnionBlockProposal}
import org.constellation.primitives.Schema.{CheckpointCacheData, EdgeHashType, SignedObservationEdge, TypedEdgeHash}
import org.constellation.primitives._
import org.constellation.{ConfigUtil, DAO}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class Round(roundData: RoundData, dao: DAO) extends Actor with ActorLogging {

  implicit val shadedDao: DAO = dao
  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  val transactionProposals: mutable.Map[FacilitatorId, LightTransactionsProposal] =
    mutable.Map()
  val checkpointBlockProposals: mutable.Map[FacilitatorId, CheckpointBlock] =
    mutable.Map()

  protected var unionTransactionProposalsTikTok: Cancellable = _

  override def receive: Receive = {
    case StartTransactionProposal(_) =>
      dao.pullTransactions(1).foreach { transactions =>
        val proposal = LightTransactionsProposal(
          roundData.roundId,
          FacilitatorId(dao.id),
          transactions.map(_.hash),
          dao.threadSafeMessageMemPool.pull(1).map(_.map(_.signedMessageData.hash)).getOrElse(Seq()),
          dao.peerInfo.flatMap(_._2.notification).toSeq
        )
        context.parent ! BroadcastLightTransactionProposal(
          roundData.peers,
          proposal
        )
        unionTransactionProposalsTikTok = scheduleUnionProposals
        self ! proposal
      }

    case proposal: LightTransactionsProposal =>
      transactionProposals += (proposal.facilitatorId -> proposal)
      if (receivedAllTransactionProposals) {
        cancelUnionTransactionProposalsTikTok()
        self ! UnionProposals
      }

    case UnionBlockProposal(roundId, facilitatorId, checkpointBlock) =>
      checkpointBlockProposals += (facilitatorId -> checkpointBlock)
      if (receivedAllCheckpointBlockProposals) {
        self ! ResolveMajorityCheckpointBlock(roundId)
      }

    case UnionProposals => unionProposals()

    case ResolveMajorityCheckpointBlock(_) => resolveMajorityCheckpointBlock()

    case msg => log.info(s"Received unknown message: $msg")
  }

  private def scheduleUnionProposals: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig("constellation.consensus.union-proposals-timeout",
                                       15.second),
      self,
      UnionProposals
    )

  private def receivedAllTransactionProposals: Boolean =
    transactionProposals.size >= roundData.peers.size + 1

  private def receivedAllCheckpointBlockProposals: Boolean =
    checkpointBlockProposals.size >= roundData.peers.size + 1

  private def cancelUnionTransactionProposalsTikTok(): Unit = {
    Option(unionTransactionProposalsTikTok).foreach(_.cancel())
  }

  def unionProposals(): Unit = {

    def resolveTransaction(hash: String, facilitatorId: FacilitatorId): IO[Transaction] =
      IO.fromFuture(IO {
        EdgeProcessor.resolveTransactions(Seq(hash), facilitatorId.id)
      }).map(_.head.transaction)

    val resolvedTxs = transactionProposals
      .values
      .flatMap(proposal ⇒ proposal.txHashes.map(hash ⇒ (hash, proposal)))
      .filterNot(p ⇒ dao.transactionService.contains(p._1).unsafeRunSync())
      .toList
      .map(p ⇒ resolveTransaction(p._1, p._2.facilitatorId))
      .sequence[IO, Transaction]
      .unsafeRunSync()

    val transactions = transactionProposals
      .values
      .flatMap(_.txHashes)
      .filterNot(hash ⇒ dao.transactionService.contains(hash).unsafeRunSync())
      .map(hash ⇒ dao.transactionService.lookup(hash).map(
        _.map(_.transaction)
      ))
      .toList
      .sequence[IO, Option[Transaction]]
      .map {
        _.flatten
          .toSet
          .union(roundData.transactions.toSet)
          .toSeq
      }
      .unsafeRunSync()

    val messages = transactionProposals
      .values
      .flatMap(_.messages)
      .map(hash ⇒ dao.messageService.lookup(hash).map(
        _.map(_.channelMessage)
      ))
      .toList
      .sequence[IO, Option[ChannelMessage]]
      .map {
        _.flatten
          .toSet
          .union(roundData.messages.toSet)
          .toSeq
      }
      .unsafeRunSync()

    val notifications = transactionProposals
      .flatMap(_._2.notifications)
      .toSet
      .union(roundData.peers.flatMap(_.notification))
      .toSeq

    val cb = CheckpointBlock.createCheckpointBlock(
      transactions ++ resolvedTxs,
      roundData.tipsSOE.map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)),
      messages,
      notifications
    )(dao.keyPair)
    val blockProposal = UnionBlockProposal(roundData.roundId, FacilitatorId(dao.id), cb)
    context.parent ! BroadcastUnionBlockProposal(roundData.peers, blockProposal)
    self ! blockProposal
  }

  def resolveMajorityCheckpointBlock(): Unit = {
    val acceptedBlock = if (checkpointBlockProposals.nonEmpty) {
      val sameBlocks = checkpointBlockProposals
        .groupBy(_._2.baseHash)
        .maxBy(_._2.size)
        ._2

      val majorityCheckpointBlock = sameBlocks.values.foldLeft(sameBlocks.head._2)(_ + _)

      val finalFacilitators = checkpointBlockProposals.keySet.map(_.id).toSet
      val cache = CheckpointCacheData(Some(majorityCheckpointBlock),
                                      height = majorityCheckpointBlock.calculateHeight())
      broadcastSignedBlockToNonFacilitators(FinishedCheckpoint(cache, finalFacilitators))

      dao.threadSafeSnapshotService.accept(cache)
      Some(majorityCheckpointBlock)
    } else None
    context.parent ! StopBlockCreationRound(roundData.roundId, acceptedBlock)
    context.stop(self)
  }

  def broadcastSignedBlockToNonFacilitators(
    finishedCheckpoint: FinishedCheckpoint
  ): Future[List[Option[FinishedCheckpointResponse]]] = {
    val allFacilitators = roundData.peers.map(p => p.peerMetadata.id -> p).toMap
    val signatureResponses = Future.sequence(
      dao.peerInfo.values.toList
        .filterNot(pd => allFacilitators.contains(pd.peerMetadata.id))
        .map { peer =>
          wrapFutureWithMetric(
            peer.client.postNonBlocking[Option[FinishedCheckpointResponse]](
              "finished/checkpoint",
              finishedCheckpoint,
              timeout = 20.seconds
            ),
            "finishedCheckpointBroadcast",
          )(dao, ec).recoverWith {
            case e: Throwable =>
              log.warning("Failure gathering signature", e)
              dao.metrics.incrementMetric(
                "formCheckpointSignatureResponseError"
              )
              Future.failed(e)
          }
        }
    )

    wrapFutureWithMetric(signatureResponses, "checkpointBlockFormation")(dao, ec)
  }

}

case class FacilitatorId(id: Schema.Id) extends AnyVal
case class RoundId(id: String) extends AnyVal

object Round {
  sealed trait RoundCommand {
    def roundId: RoundId
  }

  case object UnionProposals

  case class ResolveMajorityCheckpointBlock(roundId: RoundId) extends RoundCommand

  case class StartTransactionProposal(roundId: RoundId) extends RoundCommand

  case class LightTransactionsProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    txHashes: Seq[String],
    messages: Seq[String] = Seq(),
    notifications: Seq[PeerNotification] = Seq()
  ) extends RoundCommand

  case class UnionBlockProposal(
    roundId: RoundId,
    facilitatorId: FacilitatorId,
    checkpointBlock: CheckpointBlock
  ) extends RoundCommand

  case class RoundData(
    roundId: RoundId,
    peers: Set[PeerData],
    facilitatorId: FacilitatorId,
    transactions: Seq[Transaction],
    tipsSOE: Seq[SignedObservationEdge],
    messages: Seq[ChannelMessage]
  )

  case class StopBlockCreationRound(roundId: RoundId, maybeCB: Option[CheckpointBlock]) extends RoundCommand

  def props(roundData: RoundData, dao: DAO): Props = Props(new Round(roundData, dao))
}
