package org.constellation.util

import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.directives.BasicDirectives

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

trait SimulateTimeoutDirective extends BasicDirectives {

  private val maxMillisTimeout = 60000

  def withSimulateTimeout(enabled: Boolean)(implicit ec: ExecutionContext): Directive0 =
    extractRequestContext.flatMap { ctx =>
      if (enabled) {
        mapRouteResultWith(
          result =>
            Future {
              sleep(randomMillis)
              result
            }
        )
      } else {
        mapRouteResult(result => result)
      }
    }

  private def randomMillis = Random.nextInt(maxMillisTimeout)

  private def sleep(millis: Long) { Thread.sleep(millis) }
}
