package org.constellation.blockchain

import scala.collection.mutable.ListBuffer
import org.constellation.wallet.KeyUtils._

/**
  * Created by Wyatt on 1/22/18.
  */
object DAG {
  //TODO We prob want some sort of chain monitoring service at least for higher tiers. globalChain will serve as the local fiber chain for now
  val globalChain: ListBuffer[CheckpointBlock] = ListBuffer[CheckpointBlock]()
  val buffer: ListBuffer[BlockData] = ListBuffer[BlockData]()
  val localChain: List[BlockData] = Nil

}
