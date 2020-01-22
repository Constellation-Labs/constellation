package org.constellation.util

import org.perf4j.slf4j.Slf4JStopWatch

import scala.concurrent.{ExecutionContext, Future}

class FutureTimeTracker[T](body: => Future[T])(
  implicit executionContext: ExecutionContext) {

  private val watch = new Slf4JStopWatch()
  private val logger = watch.getLogger
  def track(tag: String): Future[T] = {
    body.andThen {
      case _ =>
        watch.stop(tag)
        logger.warn(s"Time Consumed by $tag is:${watch.getElapsedTime}")
    }
  }
}

object FutureTimeTracker {

  def apply[T](body: => Future[T])(
    implicit executionContext: ExecutionContext): FutureTimeTracker[T] =
    new FutureTimeTracker(body)
}