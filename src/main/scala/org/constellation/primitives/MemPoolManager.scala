package org.constellation.primitives

import java.security.KeyPair

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema._
import org.constellation.util.SignatureBatch

import scala.collection.{SortedSet, mutable}
import constellation._

class MemPoolManager(metricsManager: ActorRef) extends Actor {

  // import com.twitter.storehaus.cache._
  //
  //    // First, we instantiate an LRU cache with capacity 3:
  //    scala> val cache = LRUCache[Int, String](3)
  private val memPool = mutable.Seq[ResolvedTX]()

  override def receive: Receive = {
    case rtx: ResolvedTX =>
      if (memPool.contains(rtx)) {
        metricsManager ! IncrementMetric("memPoolDuplicateAdditionAttempts")
      } else {
        memPool :+ rtx
        metricsManager ! IncrementMetric("memPoolAdditions")
      }
  }
}
