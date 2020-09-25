package org.constellation.infrastructure.p2p

import cats.ApplicativeError
import cats.data.Kleisli
import cats.effect.{Blocker, Concurrent, ContextShift}
import org.constellation.ConstellationExecutionContext.unboundedBlocker
import org.constellation.infrastructure.endpoints.middlewares.PeerAuthMiddleware
import org.constellation.schema.Id
import org.constellation.session.SessionTokenService
import org.http4s.Method._
import org.http4s.Uri.{Authority, Path, RegName, Scheme}
import org.http4s.client.Client
import org.http4s.{EntityDecoder, Method, Request, Uri}

object PeerResponse {
  type PeerResponse[F[_], A] = Kleisli[F, PeerClientMetadata, A]

  case class PeerClientMetadata(host: String, port: String, id: Id)

  object PeerClientMetadata {
    def apply(host: String, port: Int, id: Id): PeerClientMetadata = PeerClientMetadata(host, port.toString, id)
  }

  def getUri(pm: PeerClientMetadata, path: String): Uri =
    Uri(scheme = Some(Scheme.http), authority = Some(Authority(host = RegName(pm.host), port = Some(pm.port.toInt))))
      .addPath(path)

  def apply[F[_], A](path: Path, method: Method) = new {

    def apply(
      client: Client[F]
    )(f: (Request[F], Client[F]) => F[A])(implicit F: Concurrent[F], C: ContextShift[F]): PeerResponse[F, A] =
      Kleisli.apply { pm =>
        val req = Request[F](method = method, uri = getUri(pm, path))
        f(req, PeerAuthMiddleware.responseVerifierMiddleware[F](pm.id)(client))
      }

    def apply(client: Client[F], sessionTokenService: SessionTokenService[F])(
      f: (Request[F], Client[F]) => F[A]
    )(implicit F: Concurrent[F], C: ContextShift[F]): PeerResponse[F, A] =
      apply(PeerAuthMiddleware.responseTokenVerifierMiddleware(client, sessionTokenService))(f)
  }

  def apply[F[_], A](path: Path) = new {

    def apply(
      client: Client[F]
    )(implicit decoder: EntityDecoder[F, A], F: Concurrent[F], C: ContextShift[F]): PeerResponse[F, A] =
      Kleisli.apply { pm =>
        val verified = PeerAuthMiddleware.responseVerifierMiddleware[F](pm.id)(client)
        verified.expect[A](getUri(pm, path))
      }

    def apply(
      client: Client[F],
      sessionTokenService: SessionTokenService[F]
    )(implicit decoder: EntityDecoder[F, A], F: Concurrent[F], C: ContextShift[F]): PeerResponse[F, A] =
      apply(PeerAuthMiddleware.responseTokenVerifierMiddleware(client, sessionTokenService))
  }

  def successful[F[_]](path: Path, errorMsg: String, method: Method = GET) = new {

    def apply(
      client: Client[F]
    )(implicit A: ApplicativeError[F, Throwable], F: Concurrent[F], C: ContextShift[F]): PeerResponse[F, Unit] =
      Kleisli
        .apply[F, PeerClientMetadata, Boolean] { pm =>
          val req = Request[F](method = method, uri = getUri(pm, path))
          val verified = PeerAuthMiddleware.responseVerifierMiddleware[F](pm.id)(client)
          verified.successful(req)
        }
        .flatMapF(a => if (a) F.unit else F.raiseError(new Throwable(errorMsg)))

    def apply(
      client: Client[F],
      sessionTokenService: SessionTokenService[F]
    )(implicit A: ApplicativeError[F, Throwable], F: Concurrent[F], C: ContextShift[F]): PeerResponse[F, Unit] =
      apply(PeerAuthMiddleware.responseTokenVerifierMiddleware(client, sessionTokenService))
  }

  def run[F[_], A, B](f: Kleisli[F, A, B], blocker: Blocker = unboundedBlocker)(
    a: A
  )(implicit contextShift: ContextShift[F]): F[B] =
    blocker.blockOn(f(a))
}
