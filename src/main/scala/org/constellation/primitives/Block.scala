package org.constellation.primitives

import java.net.InetSocketAddress

import org.constellation.primitives.Schema.Id
import org.constellation.util.ProductHash

case class Block(parentHash: String,
                 height: Long,
                 signature: String = "",
                 clusterParticipants: Set[Id] = Set(),
                 round: Long = 0L,
                 transactions: Seq[Transaction] = Seq()) extends ProductHash

//  def hash(block: Block): String = (Seq(block.height, block.parentHash) ++ block.transactions.map((t) => Transaction.hash(t)))
