package org.constellation.primitives

import java.net.InetSocketAddress

import org.constellation.p2p.PeerToPeer.Id
import org.constellation.util.ProductHash

case class Block(parentHash: String,
                 height: Long,
                 signature: String = "",
                 clusterParticipants: Set[Id] = Set(),
                 round: Long = 0L,
                 transactions: Seq[Transaction] = Seq()) extends ProductHash
// TODO: temp
case class BlockSerialized(parentHash: String,
                           height: Long,
                           signature: String = "",
                           clusterParticipants: Set[Id] = Set(),
                           round: Long = 0L,
                           transactions: Seq[Transaction] = Seq())

//  def hash(block: Block): String = (Seq(block.height, block.parentHash) ++ block.transactions.map((t) => Transaction.hash(t)))
