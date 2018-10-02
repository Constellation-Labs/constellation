package org.constellation.util

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.{Actor, ActorRef}
import org.constellation.Data
import org.constellation.primitives.Schema.InternalHeartbeat

import scala.util.Random


case object HeartbeatSubscribe

class Heartbeat(dao: Data) extends Actor {

  var actors : Seq[ActorRef] = Seq()

  val random = new Random()
  val period = 1
  val timeUnit = TimeUnit.SECONDS

  private val bufferTask = new Runnable {
    def run(): Unit = {
      if (dao.heartbeatEnabled) {
        actors.foreach {
          _ ! InternalHeartbeat
        }
      }
    }
  }

  var heartBeatMonitor: ScheduledFuture[_] = _
  var heartBeat: ScheduledThreadPoolExecutor = _

  heartBeat = new ScheduledThreadPoolExecutor(10)
  heartBeatMonitor = heartBeat.scheduleAtFixedRate(bufferTask, 1, period, timeUnit)

  def receive: Receive = {
    case HeartbeatSubscribe =>
      actors :+= sender()
  }
}