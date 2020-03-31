package org.constellation.infrastructure.p2p

import cats.{Applicative, ApplicativeError, FlatMap}
import cats.data.Kleisli
import org.constellation.infrastructure.p2p.PeerResponse.{PeerClientMetadata, PeerResponse}
import org.constellation.schema.Id
import org.http4s.{EntityDecoder, Header, Headers, MediaType, Method, Request, Response, Uri}
import org.http4s.Uri.{Authority, Path, RegName, Scheme}
import org.http4s.Method._
import org.http4s.client.Client
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._

object PeerResponse {
  type PeerResponse[F[_], A] = Kleisli[F, PeerClientMetadata, A]

  case class PeerClientMetadata(host: String, port: String, id: Id)

  object PeerClientMetadata {
    def apply(host: String, port: Int, id: Id): PeerClientMetadata = PeerClientMetadata(host, port.toString, id)
  }

  def getUri(pm: PeerClientMetadata, path: String): Uri =
    Uri(scheme = Some(Scheme.http), authority = Some(Authority(host = RegName(pm.host), port = Some(pm.port.toInt))))
      .addPath(path)

  def apply[F[_], A](path: Path, method: Method = GET)(f: Request[F] => F[A]): PeerResponse[F, A] = Kleisli.apply {
    pm =>
      val req = Request[F](method = method, uri = getUri(pm, path))
      f(req)
  }

  def apply[F[_], A](path: Path)(client: Client[F])(implicit decoder: EntityDecoder[F, A]): PeerResponse[F, A] =
    Kleisli.apply { pm =>
      client.expect[A](getUri(pm, path))
    }

  def successful[F[_]: FlatMap](path: Path, errorMsg: String, method: Method = GET)(
    client: Client[F]
  )(implicit F: ApplicativeError[F, Throwable]): PeerResponse[F, Unit] =
    Kleisli
      .apply[F, PeerClientMetadata, Boolean] { pm =>
        val req = Request[F](method = method, uri = getUri(pm, path))
        client.successful(req)
      }
      .flatMapF(a => if (a) F.unit else F.raiseError(new Throwable(errorMsg)))
}
