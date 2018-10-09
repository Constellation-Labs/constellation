package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.MemPoolManager.MemPool
import org.constellation.primitives.Schema._

object MemPoolManager {

  case class MemPool(
                    transactions: Set[Transaction],
                    checkpoints: Set[CheckpointBlock]
                    )

}

class MemPoolManager(metricsManager: ActorRef) extends Actor {

  // import com.twitter.storehaus.cache._
  //
  //    // First, we instantiate an LRU cache with capacity 3:
  //    scala> val cache = LRUCache[Int, String](3)

  override def receive: Receive = active(MemPool(Set(), Set()))

  def active(memPool: MemPool): Receive = {
    case rtx: Transaction =>
/*
      if (memPool.transactions.contains(rtx)) {
        metricsManager ! IncrementMetric("memPoolDuplicateAdditionAttempts")
      } else {
        metricsManager ! IncrementMetric("memPoolAdditions")
        context become active(memPool :+ rtx)
      }*/
  }
}
