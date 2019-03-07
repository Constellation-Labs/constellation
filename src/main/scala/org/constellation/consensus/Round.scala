package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import cats.implicits._
import constellation.{wrapFutureWithMetric, _}
import org.constellation.{ConfigUtil, DAO}
import org.constellation.consensus.EdgeProcessor.{FinishedCheckpoint, FinishedCheckpointResponse}
import org.constellation.consensus.Round._
import org.constellation.primitives.Schema.{
    CheckpointCacheData,
    EdgeHashType,
    SignedObservationEdge,
    TypedEdgeHash
  }
import org.constellation.primitives._
import org.constellation.util.Validation.EnrichedFuture

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

class Round(roundData: RoundData, dao: DAO) extends Actor with ActorLogging {

  protected val transactionProposals: mutable.Map[FacilitatorId, TransactionsProposal] =
      mutable.Map()
  protected val checkpointBlockProposals: mutable.Map[FacilitatorId, CheckpointBlock] =
      mutable.Map()

  protected var unionTransactionProposalsTikTok: Cancellable = _

  override def receive: Receive = {
    case StartTransactionProposal(_) =>
      dao.pullTransactions(1).foreach { transactions =>
        val proposal = TransactionsProposal(roundData.roundId, FacilitatorId(dao.id), transactions)
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
    // TODO: wkoszycki use distinct instead
    val cb = CheckpointBlock.createCheckpointBlock(
      transactionProposals.values
        .flatMap(_.transactions)
        .toSet
        .union(roundData.transactions.toSet)
        .toSeq,
      roundData.tipsSOE.map(soe => TypedEdgeHash(soe.hash, EdgeHashType.CheckpointHash)),
      // TODO: wkoszycki should those messages be combined with local node messages?
      roundData.messages
    )(dao.keyPair)
    val blockProposal = UnionBlockProposal(roundData.roundId, FacilitatorId(dao.id), cb)
    context.parent ! BroadcastUnionBlockProposal(roundData.peers, blockProposal)
    self ! blockProposal
  }

  def resolveMajorityCheckpointBlock(): Unit = {
      implicit val shadedDao: DAO = dao
      implicit val ec: ExecutionContextExecutor = dao.edgeExecutionContext

      // TODO: wkoszycki what do we do with such condition
      //      1 -> [A, B, C, D]
      //      2 -> [E, f, C, D]
      if (checkpointBlockProposals.nonEmpty) {
        val sameBlocks = checkpointBlockProposals
          .groupBy(_._2.baseHash)
          .maxBy(u => u._2.size)
          ._2

        val majorityCheckpointBlock = sameBlocks.values.foldLeft(sameBlocks.head._2)(_ + _)

        val finalFacilitators = checkpointBlockProposals.keySet.map(_.id).toSet
        val cache = CheckpointCacheData(Some(majorityCheckpointBlock),
                                        height = majorityCheckpointBlock.calculateHeight())
        val allFacilitators = roundData.peers.map(p => p.peerMetadata.id -> p).toMap

        val finishedResponses = dao.peerInfo.values
          .filterNot(pd => allFacilitators.contains(pd.peerMetadata.id))
          .toList
          .map { peer =>
            wrapFutureWithMetric(
              peer.client.postNonBlocking[Option[FinishedCheckpointResponse]](
                "finished/checkpoint",
                FinishedCheckpoint(cache, finalFacilitators),
                timeout = 20.seconds
              ),
              "finishedCheckpointBroadcast",
            )
          }
        // TODO: wkoszycki validate and update metrics on complete
        dao.threadSafeTipService.accept(cache)
      }
      context.parent ! StopBlockCreationRound(roundData.roundId)
      context.stop(self)
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
  // TODO: wkoszycki should be declared in RoundManager perhaps
  case class BroadcastTransactionProposal(peers: Set[PeerData],
                                          transactionsProposal: TransactionsProposal)
  case class BroadcastUnionBlockProposal(peers: Set[PeerData], proposal: UnionBlockProposal)

  case class TransactionsProposal(roundId: RoundId,
                                  facilitatorId: FacilitatorId,
                                  transactions: Seq[Transaction])
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
