package org.constellation.blockchain

import scala.collection.mutable.ListBuffer

// TODO: Mutable is okay here for now, but these may need to invoke a DB / off heap queue / cache later

/**
  * Created by Wyatt on 1/22/18.
  */
class DAG {
  //TODO We prob want some sort of chain monitoring service at least for higher tiers. globalChain will serve as the local fiber chain for now
  val globalChain: ListBuffer[CheckpointBlock] = ListBuffer.empty[CheckpointBlock]
  val buffer: ListBuffer[BlockData] = ListBuffer.empty[BlockData]
  val localChain: List[BlockData] = Nil

}
