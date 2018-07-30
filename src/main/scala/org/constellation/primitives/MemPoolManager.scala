package org.constellation.primitives

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._

class MemPoolManager(metricsManager: ActorRef) extends Actor {

  // import com.twitter.storehaus.cache._
  //
  //    // First, we instantiate an LRU cache with capacity 3:
  //    scala> val cache = LRUCache[Int, String](3)

  override def receive = active(Seq.empty)

  def active(memPool: Seq[ResolvedTX]): Receive = {
    case rtx: ResolvedTX =>
      if (memPool.contains(rtx)) {
        metricsManager ! IncrementMetric("memPoolDuplicateAdditionAttempts")
      } else {
        metricsManager ! IncrementMetric("memPoolAdditions")
        context become active(memPool :+ rtx)
      }
  }
}
