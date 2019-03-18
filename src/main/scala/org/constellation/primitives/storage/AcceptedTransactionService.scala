package org.constellation.primitives.storage

import org.constellation.primitives.{TransactionCacheData, TransactionSerialized}

import scala.collection.mutable

class AcceptedTransactionService(size: Int = 50000)
    extends StorageService[TransactionCacheData](size) {
  private val queue = mutable.Queue[TransactionSerialized]()
  private val maxQueueSize = 20

  override def putSync(
    key: String,
    cache: TransactionCacheData
  ): TransactionCacheData = {
    val tx = TransactionSerialized(cache.transaction)
    queue.synchronized {
      if (queue.size == maxQueueSize) {
        queue.dequeue()
      }

      queue.enqueue(tx)
      super.putSync(key, cache)
    }
  }

  def getLast20TX = queue.reverse
}
