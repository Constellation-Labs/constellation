package org.constellation.primitives

import java.util.concurrent.Semaphore

import akka.actor.ActorRef
import com.typesafe.scalalogging.StrictLogging
import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.InternalHeartbeat
import org.constellation.util.Periodic

import scala.concurrent.Future
import scala.util.{Random, Try}

class InternalHeartbeatTrigger[T](nodeActor: ActorRef, periodSeconds: Int = 10)(implicit dao: DAO)
    extends Periodic[Try[Unit]]("InternalHeartbeatTrigger", periodSeconds)
    with StrictLogging {

  def trigger(): Future[Try[Unit]] = {
    Thread.currentThread().setName("InternalHeartbeatTrigger")
    dao.cluster.internalHearthbeat(round).map(_ => Try(())).unsafeToFuture
  }
}
