package org.constellation.util

import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.Future
import scala.util.Try // Try unused

abstract class Periodic[T](threadName: String, periodSeconds: Int = 1) {

  private val executor = new ScheduledThreadPoolExecutor(1)

  var round: Long = 0L

  private var lastExecution: Future[T] = Future.failed(new RuntimeException("No executions yet"))

  def trigger(): Future[T]

  /**
    * Recalculates window based / periodic metrics
    */
  private val task = new Runnable {

    def run(): Unit = {
      round += 1
      Thread.currentThread().setName(threadName)
      if (lastExecution.isCompleted) {
        lastExecution = trigger()
      }
    }
  }

  // We may get rid of Akka so using this instead of the context scheduler
  private val scheduledFuture =
    executor.scheduleAtFixedRate(task, 1, periodSeconds, TimeUnit.SECONDS)

  def shutdown(): Boolean = scheduledFuture.cancel(false)

}
