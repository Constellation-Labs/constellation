package org.constellation.primitives.storage

import com.twitter.storehaus.cache.MutableLRUCache
import scala.collection.JavaConverters._

class ExtendedMutableLRUCache[K, V](capacity: Int) extends MutableLRUCache[K, V](capacity){

  def asImmutableMap(): Map[K,V] = {
    m.asScala.toMap
  }

}
