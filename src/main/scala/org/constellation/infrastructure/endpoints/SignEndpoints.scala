package org.constellation.infrastructure.endpoints

import java.security.KeyPair

import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import io.circe.syntax._
import org.constellation.p2p.{Cluster, PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.util.{HostPort, SignHelp, SingleHashSignature}
import org.http4s.{HttpRoutes, _}
import org.http4s.circe._
import org.http4s.dsl.Http4sDsl
import PeerAuthSignRequest._
import SingleHashSignature._
import PeerRegistrationRequest._
import HostPort._
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.schema.Id
import org.constellation.session.Registration.JoinRequestPayload

class SignEndpoints[F[_]](implicit F: Concurrent[F], C: ContextShift[F]) extends Http4sDsl[F] {

  def publicPeerEndpoints(keyPair: KeyPair, cluster: Cluster[F]) =
    signEndpoint(keyPair) <+>
      registerEndpoint(cluster) <+>
      getRegistrationRequest(cluster)

  private def signEndpoint(keyPair: KeyPair): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "sign" =>
      (for {
        signRequest <- req.decodeJson[PeerAuthSignRequest]
        hash = signRequest.salt.toString
        signature = SignHelp.hashSign(hash, keyPair)
      } yield SingleHashSignature(hash, signature).asJson).flatMap(Ok(_))
  }

  def ownerEndpoints(cluster: Cluster[F]) = joinEndpoint(cluster) <+> leaveEndpoint(cluster)

  private def registerEndpoint(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "register" =>
      (for {
        registrationRequest <- req.decodeJson[PeerRegistrationRequest]
        ip = req.remoteAddr.getOrElse("")
        _ <- cluster.pendingRegistration(ip, registrationRequest)
      } yield ()) >> Ok()
  }

  private def joinEndpoint(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root / "join" =>
      (for {
        hostPort <- req.decodeJson[JoinRequestPayload]
        _ <- F.start(C.shift >> cluster.join(PeerClientMetadata(hostPort.host, hostPort.port, Id(hostPort.id))))
      } yield ()) >> Ok()
  }

  private def leaveEndpoint(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case POST -> Root / "leave" =>
      F.start(C.shift >> cluster.leave(F.unit)) >> Ok()
  }

  private def getRegistrationRequest(cluster: Cluster[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "registration" / "request" =>
      cluster.pendingRegistrationRequest.map(_.asJson).flatMap(Ok(_))
  }

}

object SignEndpoints {

  def publicPeerEndpoints[F[_]: Concurrent: ContextShift](keyPair: KeyPair, cluster: Cluster[F]): HttpRoutes[F] =
    new SignEndpoints[F]().publicPeerEndpoints(keyPair, cluster)

  def ownerEndpoints[F[_]: Concurrent: ContextShift](cluster: Cluster[F]): HttpRoutes[F] =
    new SignEndpoints[F]().ownerEndpoints(cluster)
}
