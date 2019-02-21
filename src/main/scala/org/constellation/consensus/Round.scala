package org.constellation.consensus
import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import org.constellation.primitives.{CheckpointBlock, Schema}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class Round(id: RoundId) extends Actor with ActorLogging {
  var proposals: Map[FacilitatorId, CheckpointBlock] = Map()

  override def preStart(): Unit = {
    context.parent ! NotifyFacilitators(id)

    // TODO: pull transactions and broadcast proposal
    // dao.threadSafeTXMemPool.pull(dao.minCheckpointFormationThreshold)

  }

  override def receive: Receive = {
    case ReceivedProposal(_, facilitatorId, cb) =>
      storeProposal(facilitatorId, cb)

    case UnionProposals(_) => unionProposals()

    case _ => log.info("Received unknown message")
  }

  def scheduleUnionProposals: Cancellable =
    context.system.scheduler.scheduleOnce(15.seconds, self, UnionProposals(id))

  def storeProposal(facilitatorId: FacilitatorId, cb: CheckpointBlock): Unit = {
    proposals = proposals + (facilitatorId -> cb)
  }

  def unionProposals(): Unit = {
    log.debug(proposals.toString())
    log.info("Union proposals and broadcast majority unioned block")
  }

}

object Round {
  def props(id: RoundId): Props = Props(new Round(id))
}
