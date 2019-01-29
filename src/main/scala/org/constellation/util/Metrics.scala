package org.constellation.util

object Metrics {

}


class Metrics(periodSeconds: Int) {
  import java.util.concurrent._

  val ex = new ScheduledThreadPoolExecutor(1)
  val task = new Runnable {
    def run() = println("Beep!")
  }
  val f = ex.scheduleAtFixedRate(task, 1, 1, TimeUnit.SECONDS)
  f.cancel(false)

}