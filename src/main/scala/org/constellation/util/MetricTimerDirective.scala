package org.constellation.util

import java.util.concurrent.TimeUnit
import akka.http.scaladsl.server.directives.BasicDirectives
import akka.http.scaladsl.server.{Directive0, RequestContext}
import com.typesafe.scalalogging.Logger
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.search.MeterNotFoundException
import scala.util.Try

trait MetricTimerDirective extends BasicDirectives {

  protected implicit val logger: Logger

  def withTimer(name: String): Directive0 =
    timer(name)

  private[util] def timer(name: String): Directive0 =
    extractRequestContext.flatMap { ctx ⇒
      val timer = findAndRegisterTimer(getMetricName(name, ctx))
      mapRouteResult { result ⇒
        timer.stop()
        result
      }
    }

  protected def findAndRegisterTimer(name: String): MetricTimer = {
    Try(io.micrometer.core.instrument.Metrics.globalRegistry.get(name).timer()).recover {
      case _: MeterNotFoundException => io.micrometer.core.instrument.Metrics.globalRegistry.timer(name)
      case e: Exception              => logger.error(e.getMessage)
    }
    new MetricTimer(io.micrometer.core.instrument.Metrics.timer(name))
  }

  protected def getMetricName(name: String, ctx: RequestContext): String =
    name + "." +
      ctx.request.method.value.toLowerCase + "." +
      ctx.request.uri.path.toString.drop(1).split("/").take(2).mkString(".")

  class MetricTimer(t: Timer) {
    val timer = t
    val startTimeMs = System.currentTimeMillis()
    def stop() = timer.record(System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS)
  }

}
