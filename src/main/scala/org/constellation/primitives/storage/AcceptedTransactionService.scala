package org.constellation.primitives.storage

import org.constellation.primitives.Schema.{TransactionCacheData, TransactionSerialized}

import scala.collection.mutable

class TransactionService(size: Int = 50000) extends StorageService[TransactionCacheData](size)

class AcceptedTransactionService(size: Int = 50000)
    extends StorageService[TransactionCacheData](size) {
  private val queue = mutable.Queue[TransactionSerialized]()
  private val maxQueueSize = 20

  override def put(
    key: String,
    cache: TransactionCacheData
  ): TransactionCacheData = {
    val tx = TransactionSerialized(cache.transaction)
    queue.synchronized {
      if (queue.size == maxQueueSize) {
        queue.dequeue()
      }

      queue.enqueue(tx)
      super.put(key, cache)
    }
  }

  def getLast20TX = queue.reverse
}
