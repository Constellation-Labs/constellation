package org.constellation.infrastructure.endpoints

import better.files.File
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Printer
import io.circe.syntax._
import org.constellation.ConstellationExecutionContext
import org.constellation.ConstellationNode.getClass
import org.constellation.storage.MessageService
import org.constellation.util.ServeUI
import org.http4s.{HttpRoutes, _}
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl

import org.constellation.primitives.ChannelMessageMetadata._

class UIEndpoints[F[_]](implicit F: Concurrent[F], C: ContextShift[F]) extends Http4sDsl[F] {

  val logger = Slf4jLogger.getLogger[F]
  val blockingEc = Blocker.liftExecutionContext(ConstellationExecutionContext.unbounded)

  def ownerEndpoints(messageService: MessageService[F]) =
    jsRequestEndpoint() <+> imageEndpoint() <+> serveMainPageEndpoint(messageService)

  private def jsRequestEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> "js" /: path ~ "js" =>
        if (path.toString.startsWith("/ui-"))
          StaticFile.fromResource[F]("ui" + path + ".js", blockingEc).getOrElseF(NotFound())
        else NotFound()
    }

  private def imageEndpoint(): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "img" / name => StaticFile.fromResource[F](s"ui/img/$name", blockingEc).getOrElseF(NotFound())
    }

  import scalatags.Text.all._

  private def serveMainPageEndpoint(messageService: MessageService[F]): HttpRoutes[F] =
    HttpRoutes.of[F] {
      case GET -> Root / "favicon.ico" => {
        StaticFile
          .fromResource[F]("ui/img/favicon.ico", blockingEc)
          .getOrElseF(NotFound())
      }
      case GET -> Root => Ok(UI.defaultIndexPage(""), Header("Content-type", "text/html"))
      case GET -> Root / "view" / page =>
        Ok(UI.defaultIndexPage(page), Header("Content-type", "text/html"))
      case GET -> Root / "view" / _ / "channel" =>
        Ok(UI.defaultIndexPage(""), Header("Content-type", "text/html"))
      case GET -> Root / "view" / msgHash / "message" =>
        messageService.lookup(msgHash).flatMap { msgs =>
          Ok(
            UI.defaultIndexPage(
                "",
                Some(
                  div(
                    id := "message-view",
                    msgs.asJson.printWith(Printer.spaces2)
                  )
                )
              )
              .asJson,
            Header("Content-type", "text/html")
          )
        }
    }

  object UI extends ServeUI
}

object UIEndpoints {

  def ownerEndpoints[F[_]: Concurrent: ContextShift](messageService: MessageService[F]): HttpRoutes[F] =
    new UIEndpoints[F]().ownerEndpoints(messageService)
}
