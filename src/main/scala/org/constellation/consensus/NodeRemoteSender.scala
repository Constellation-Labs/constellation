package org.constellation.consensus

import akka.actor.{Actor, Props}
import org.constellation.consensus.CrossTalkConsensus.NotifyFacilitators
import org.constellation.consensus.RoundManager.{
  BroadcastLightTransactionProposal,
  BroadcastSelectedUnionBlock,
  BroadcastUnionBlockProposal
}

object NodeRemoteSender {

  def props(nodeRemoteSender: NodeRemoteSender): Props =
    Props(new ForwardingNodeSender(nodeRemoteSender))
}

trait NodeRemoteSender {
  def notifyFacilitators(cmd: NotifyFacilitators): Unit

  def broadcastLightTransactionProposal(cmd: BroadcastLightTransactionProposal): Unit

  def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): Unit

  def broadcastSelectedUnionBlock(cmd: BroadcastSelectedUnionBlock): Unit
}

class ForwardingNodeSender(nodeRemoteSender: NodeRemoteSender) extends NodeRemoteSender with Actor {

  override def receive: Receive = {
    case cmd: NotifyFacilitators =>
      notifyFacilitators(cmd)

    case cmd: BroadcastLightTransactionProposal =>
      broadcastLightTransactionProposal(cmd)

    case cmd: BroadcastUnionBlockProposal =>
      broadcastBlockUnion(cmd)

    case cmd: BroadcastSelectedUnionBlock =>
      broadcastSelectedUnionBlock(cmd)
  }

  def notifyFacilitators(cmd: NotifyFacilitators): Unit =
    nodeRemoteSender.notifyFacilitators(cmd)

  def broadcastLightTransactionProposal(cmd: BroadcastLightTransactionProposal): Unit =
    nodeRemoteSender.broadcastLightTransactionProposal(cmd)

  def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): Unit =
    nodeRemoteSender.broadcastBlockUnion(cmd)

  def broadcastSelectedUnionBlock(cmd: BroadcastSelectedUnionBlock): Unit =
    nodeRemoteSender.broadcastSelectedUnionBlock(cmd)
}
