package org.constellation.consensus

import akka.actor.{Actor, Props}
import org.constellation.consensus.CrossTalkConsensus.NotifyFacilitators
import org.constellation.consensus.RoundManager.{
    BroadcastTransactionProposal,
    BroadcastUnionBlockProposal
  }

object NodeRemoteSender {

    def props(nodeRemoteSender: NodeRemoteSender): Props =
      Props(new ForwardingNodeSender(nodeRemoteSender))
  }

trait NodeRemoteSender {
    def notifyFacilitators(cmd: NotifyFacilitators): Unit

    def broadcastTransactionProposal(cmd: BroadcastTransactionProposal): Unit

    def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): Unit
  }

class ForwardingNodeSender(nodeRemoteSender: NodeRemoteSender)
      extends NodeRemoteSender
      with Actor {

    override def receive: Receive = {
      case cmd: NotifyFacilitators =>
        notifyFacilitators(cmd)
      case cmd: BroadcastTransactionProposal =>
        broadcastTransactionProposal(cmd)
      case cmd: BroadcastUnionBlockProposal =>
        broadcastBlockUnion(cmd)
    }

    def notifyFacilitators(cmd: NotifyFacilitators): Unit = {
      nodeRemoteSender.notifyFacilitators(cmd)
    }
    def broadcastTransactionProposal(cmd: BroadcastTransactionProposal): Unit = {
      nodeRemoteSender.broadcastTransactionProposal(cmd)
    }

    def broadcastBlockUnion(cmd: BroadcastUnionBlockProposal): Unit = {
      nodeRemoteSender.broadcastBlockUnion(cmd)
    }
  }