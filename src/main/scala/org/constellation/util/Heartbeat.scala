package org.constellation.util

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import constellation.futureTryWithTimeoutMetric
import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.primitives.RandomTransactionManager
import org.constellation.primitives.Schema.{InternalHeartbeat, NodeState}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Try

case object HeartbeatSubscribe
case object TriggerHeartbeats

class Heartbeat()(implicit dao: DAO) extends Actor {

  val period = 1

  var round = 0L

  var lastRTMExecution : Future[Try[Any]] = Future.successful(Try{})

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

    case InternalHeartbeat(_) =>

      Snapshot.triggerSnapshot(round)

      if (lastRTMExecution.isCompleted) {
        lastRTMExecution = RandomTransactionManager.trigger(round)
      }


  }

  def receive: Receive = active(Set(self))
}