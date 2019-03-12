package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import cats.implicits._
import constellation.{wrapFutureWithMetric, _}
import org.constellation.consensus.EdgeProcessor.{FinishedCheckpoint, FinishedCheckpointResponse}
import org.constellation.consensus.Round._
import org.constellation.consensus.RoundManager.{
  BroadcastTransactionProposal,
  BroadcastUnionBlockProposal
}
import org.constellation.primitives.Schema.{
  CheckpointCacheData,
  EdgeHashType,
  SignedObservationEdge,
  TypedEdgeHash
}
import org.constellation.primitives._
import org.constellation.{ConfigUtil, DAO}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}

class Round(roundData: RoundData, dao: DAO) extends Actor with ActorLogging {

  implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

  val transactionProposals: mutable.Map[FacilitatorId, TransactionsProposal] =
    mutable.Map()
  val checkpointBlockProposals: mutable.Map[FacilitatorId, CheckpointBlock] =
    mutable.Map()

  protected var unionTransactionProposalsTikTok: Cancellable = _

  override def receive: Receive = {
    case StartTransactionProposal(_) =>
      dao.pullTransactions(1).foreach { transactions =>
        val proposal = TransactionsProposal(
          roundData.roundId, FacilitatorId(dao.id), transactions,
          dao.threadSafeMessageMemPool.pull(1).getOrElse(Seq())
        )
        context.parent ! BroadcastTransactionProposal(
          roundData.peers,
          proposal
        )
        unionTransactionProposalsTikTok = scheduleUnionProposals
        self ! proposal
      }
    case proposal: TransactionsProposal =>
      transactionProposals += (proposal.facilitatorId -> proposal)
      if (transactionProposals.size >= roundData.peers.size + 1) {
        Option(unionTransactionProposalsTikTok).foreach(_.cancel())
        self ! UnionProposals
      }
    case UnionBlockProposal(roundId, facilitatorId, checkpointBlock) =>
      checkpointBlockProposals += (facilitatorId -> checkpointBlock)
      if (checkpointBlockProposals.size >= roundData.peers.size + 1) {
        self ! ResolveMajorityCheckpointBlock(roundId)
      }
    case UnionProposals                    => unionProposals()
    case ResolveMajorityCheckpointBlock(_) => resolveMajorityCheckpointBlock()
    case msg                               => log.info(s"Received unknown message: $msg")
  }

  private def scheduleUnionProposals: Cancellable =
    context.system.scheduler.scheduleOnce(
      ConfigUtil.getDurationFromConfig("constellation.consensus.union-proposals-timeout",
                                       15.second),
      self,
      UnionProposals
    )

  def unionProposals(): Unit = {

    val transactions = transactionProposals
      .flatMap(_._2.transactions)
      .toSet
      .union(roundData.transactions.toSet)
      .toSeq

    val messages = transactionProposals
      .flatMap(_._2.messages)
      .toSet
      .union(roundData.messages.toSet)
      .toSeq

    val cb = CheckpointBlock.createCheckpointBlock(
      transactions,
      roundData.tipsSOE.map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)),
      messages
    )(dao.keyPair)
    val blockProposal = UnionBlockProposal(roundData.roundId, FacilitatorId(dao.id), cb)
    context.parent ! BroadcastUnionBlockProposal(roundData.peers, blockProposal)
    self ! blockProposal
  }

  def resolveMajorityCheckpointBlock(): Unit = {
    implicit val shadedDao: DAO = dao

    if (checkpointBlockProposals.nonEmpty) {
      val sameBlocks = checkpointBlockProposals
        .groupBy(_._2.baseHash)
        .maxBy(_._2.size)
        ._2

      val majorityCheckpointBlock = sameBlocks.values.foldLeft(sameBlocks.head._2)(_ + _)

      val finalFacilitators = checkpointBlockProposals.keySet.map(_.id).toSet
      val cache = CheckpointCacheData(Some(majorityCheckpointBlock),
                                      height = majorityCheckpointBlock.calculateHeight())
      broadcastSignedBlockToNonFacilitators(FinishedCheckpoint(cache, finalFacilitators))

      dao.threadSafeTipService.accept(cache)
    }
    context.parent ! StopBlockCreationRound(roundData.roundId)
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

  case class TransactionsProposal(roundId: RoundId,
                                  facilitatorId: FacilitatorId,
                                  transactions: Seq[Transaction],
                                  messages: Seq[ChannelMessage] = Seq()
                                 )
      extends RoundCommand

  case class UnionBlockProposal(roundId: RoundId,
                                facilitatorId: FacilitatorId,
                                checkpointBlock: CheckpointBlock)
      extends RoundCommand

  case class RoundData(roundId: RoundId,
                       peers: Set[PeerData],
                       facilitatorId: FacilitatorId,
                       transactions: Seq[Transaction],
                       tipsSOE: Seq[SignedObservationEdge],
                       messages: Seq[ChannelMessage])

  case class StopBlockCreationRound(roundId: RoundId) extends RoundCommand

  def props(roundData: RoundData, dao: DAO): Props = Props(new Round(roundData, dao))
}
