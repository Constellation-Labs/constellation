package org.constellation.storage

import java.util.concurrent.ConcurrentLinkedQueue

// TODO: Put transaction tracker / block tracker here

/**
  * For keeping track of recent data for display in UI
  * @param maxLength: Number of elements of T to return
  * @tparam T: Type of data being stored
  */
class RecentDataTracker[T](maxLength: Int = 100) {

  private val queue = new ConcurrentLinkedQueue[T]()

  def put(item: T): Boolean = {
    if (queue.size() > maxLength) {
      queue.poll()
    }
    queue.add(item)
  }

  def getAll: Array[T] =
    queue.toArray().asInstanceOf[Array[T]]

}
