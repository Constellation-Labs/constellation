package org.constellation.alerts

import java.time.LocalDateTime

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Timer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.syntax._
import org.constellation.alerts.primitives.Incident._
import org.constellation.alerts.primitives.{Alert, Incident, IncidentPayload, Link}
import org.http4s.circe._
import org.http4s.client.blaze._
import org.http4s.{Header, Request, _}

import scala.concurrent.ExecutionContext.Implicits.global

class PagerDutyClient[F[_]: Concurrent: ContextShift: ConcurrentEffect: Timer](
  selfIp: String,
  integrationKey: String
) {

  private val client = BlazeClientBuilder[F](global)
  private val uri = uri"https://events.pagerduty.com/v2/enqueue"
  private val header = Header("X-Routing-Key", integrationKey)
  private val logger = Slf4jLogger.getLogger[F]

  def sendAlert(alert: Alert): F[Unit] = client.resource.use { c =>
    c.status(createRequest(alert)) // TODO: Retry in case of error
  }

  private def createRequest(alert: Alert): Request[F] = {
    val payload = IncidentPayload(
      summary = s"${alert.title} [${selfIp}]",
      timestamp = LocalDateTime.now().toString,
      source = selfIp,
      severity = alert.severity,
      custom_details = alert,
    )

    val body = Incident(
      payload = payload,
      links = Seq(
        Link(s"http://${selfIp}:9000/", s"Metrics - http://${selfIp}:9000/")
      )
    ).asJson


    Request(method = Method.POST, uri = uri)
      .withHeaders(header)
      .withEntity(body)
  }
}
