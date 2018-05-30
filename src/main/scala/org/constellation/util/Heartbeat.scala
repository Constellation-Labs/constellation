package org.constellation.util

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import org.constellation.primitives.Schema.InternalHeartbeat

import scala.util.{Failure, Success, Try}

trait Heartbeat {

  val self: ActorRef
  val heartbeatEnabled: Boolean
  val logger: Logger
  var lastHeartbeatTime: Long
  val periodSeconds = 3

  private val bufferTask = new Runnable { def run(): Unit = {
    self ! InternalHeartbeat
  } }

  var heartBeatMonitor: ScheduledFuture[_] = _
  var heartBeat: ScheduledThreadPoolExecutor = _

  if (heartbeatEnabled) {
    heartBeat = new ScheduledThreadPoolExecutor(10)
    heartBeatMonitor = heartBeat.scheduleAtFixedRate(bufferTask, 1, periodSeconds, TimeUnit.SECONDS)
  }

  def processHeartbeat[T](t: => T): Option[T] = {
    if (System.currentTimeMillis() > (lastHeartbeatTime + periodSeconds*1000 - 500)) {
      Try {
        lastHeartbeatTime = System.currentTimeMillis()
        t
      } match {
        case Failure(e) => e.printStackTrace(); None
        case Success(x) => Some(x)
      }
    } else {
      logger.debug("Heartbeat overwhelmed, messages backing up, skipping internal heartbeat until sufficient time elapsed.")
      None
    }
  }

}
