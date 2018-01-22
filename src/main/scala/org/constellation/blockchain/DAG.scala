package org.constellation.blockchain

import scala.collection.mutable.ListBuffer

/**
  * Created by Wyatt on 1/22/18.
  */
object DAG {
  //TODO We prob want some sort of chain monitoring service at least for higher tiers. globalChain will serve as the local fiber chain for now
  val globalChain: ListBuffer[Block] = ListBuffer[Block]()

  def calculateHash(index: Int, previousHash: String, timestamp: Long, data: String) = s"$index:$previousHash:$timestamp:$data"
}
