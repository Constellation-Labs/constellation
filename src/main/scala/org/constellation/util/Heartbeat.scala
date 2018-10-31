package org.constellation.util

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import org.constellation.DAO
import org.constellation.primitives.Schema.InternalHeartbeat

import scala.concurrent.duration.Duration

case object HeartbeatSubscribe
case object TriggerHeartbeats

class Heartbeat(dao: DAO) extends Actor {

  val period = 1

  var round = 0L

  context.system.scheduler.schedule(Duration.Zero, Duration(period, TimeUnit.SECONDS), self, TriggerHeartbeats)(context.dispatcher)

  def active(actors : Set[ActorRef]): Receive = {

    case HeartbeatSubscribe =>
      context.become(active(actors + sender()))

    case TriggerHeartbeats =>
      if (dao.heartbeatEnabled) {
        round += 1L
        actors.foreach {
          _ ! InternalHeartbeat(round)
        }
      }
  }

  def receive: Receive = active(Set())
}