package org.constellation.infrastructure.p2p

import cats.ApplicativeError
import cats.data.Kleisli
import cats.effect.{Concurrent, ContextShift, Sync}
import org.constellation.infrastructure.endpoints.middlewares.PeerAuthMiddleware
import org.constellation.schema.Id
import org.http4s.{EntityDecoder, Method, Request, Uri}
import org.http4s.Uri.{Authority, Path, RegName, Scheme}
import org.http4s.Method._
import org.http4s.client.Client

object PeerResponse {
  type PeerResponse[F[_], A] = Kleisli[F, PeerClientMetadata, A]

  case class PeerClientMetadata(host: String, port: String, id: Id)

  object PeerClientMetadata {
    def apply(host: String, port: Int, id: Id): PeerClientMetadata = PeerClientMetadata(host, port.toString, id)
  }

  def getUri(pm: PeerClientMetadata, path: String): Uri =
    Uri(scheme = Some(Scheme.http), authority = Some(Authority(host = RegName(pm.host), port = Some(pm.port.toInt))))
      .addPath(path)

  def apply[F[_]: Concurrent: ContextShift, A](path: Path, client: Client[F], method: Method = GET)(
    f: (Request[F], Client[F]) => F[A]
  ): PeerResponse[F, A] =
    Kleisli.apply { pm =>
      val req = Request[F](method = method, uri = getUri(pm, path))
      f(req, PeerAuthMiddleware.responseVerifierMiddleware[F](pm.id)(client))
    }

  def apply[F[_]: Concurrent: ContextShift, A](
    path: Path
  )(client: Client[F])(implicit decoder: EntityDecoder[F, A]): PeerResponse[F, A] =
    Kleisli.apply { pm =>
      val verified = PeerAuthMiddleware.responseVerifierMiddleware[F](pm.id)(client)
      verified.expect[A](getUri(pm, path))
    }

  def successful[F[_]: Concurrent: ContextShift](path: Path, errorMsg: String, method: Method = GET)(
    client: Client[F]
  )(implicit F: ApplicativeError[F, Throwable]): PeerResponse[F, Unit] =
    Kleisli
      .apply[F, PeerClientMetadata, Boolean] { pm =>
        val req = Request[F](method = method, uri = getUri(pm, path))
        val verified = PeerAuthMiddleware.responseVerifierMiddleware[F](pm.id)(client)
        verified.successful(req)
      }
      .flatMapF(a => if (a) F.unit else F.raiseError(new Throwable(errorMsg)))
}
