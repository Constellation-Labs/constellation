package org.constellation.util

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.ActorRef
import com.typesafe.scalalogging.Logger
import org.constellation.primitives.Schema.{InternalBundleHeartbeat, InternalHeartbeat}

import scala.util.{Failure, Success, Try}

class BundleHeartbeat(

                       val self: ActorRef,
                       val heartbeatEnabled: Boolean,
                       val logger: Logger,
                       override val period: Int = 500,
                       override val timeUnit: TimeUnit = TimeUnit.MILLISECONDS,
                       override val loggerEnabled: Boolean = false,
                       override val heartbeatMessage: Any = InternalBundleHeartbeat
                     ) extends Heartbeat

trait Heartbeat {

  val self: ActorRef
  val heartbeatEnabled: Boolean
  val logger: Logger
  var lastHeartbeatTime: Long = System.currentTimeMillis()
  val period = 2
  val timeUnit = TimeUnit.SECONDS
  val loggerEnabled: Boolean = true
  val heartbeatMessage: Any = InternalHeartbeat

  private val bufferTask = new Runnable { def run(): Unit = {
    self ! heartbeatMessage
  } }

  var heartBeatMonitor: ScheduledFuture[_] = _
  var heartBeat: ScheduledThreadPoolExecutor = _

  if (heartbeatEnabled) {
    heartBeat = new ScheduledThreadPoolExecutor(10)
    heartBeatMonitor = heartBeat.scheduleAtFixedRate(bufferTask, 1, period, timeUnit)
  }

  def processHeartbeat[T](t: => T): Option[T] = {
    if (System.currentTimeMillis() > (lastHeartbeatTime + period*1000 - 500)) {
      Try {
        lastHeartbeatTime = System.currentTimeMillis()
        t
      } match {
        case Failure(e) => e.printStackTrace(); None
        case Success(x) => Some(x)
      }
    } else {
    //  if (loggerEnabled)
    //    logger.debug("Heartbeat overwhelmed, messages backing up, skipping internal heartbeat until sufficient time elapsed.")
      None
    }
  }

}
