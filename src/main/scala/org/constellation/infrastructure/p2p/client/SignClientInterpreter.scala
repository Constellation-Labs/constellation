package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.SignClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.p2p.{PeerAuthSignRequest, PeerRegistrationRequest}
import org.constellation.schema.v2.signature.SingleHashSignature
import org.http4s.Method._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class SignClientInterpreter[F[_]: ContextShift](client: Client[F])(implicit F: Concurrent[F])
    extends SignClientAlgebra[F] {

  def sign(authSignRequest: PeerAuthSignRequest): PeerResponse[F, SingleHashSignature] =
    PeerResponse("sign", POST)(client) { (req, c) =>
      c.expect[SingleHashSignature](req.withEntity(authSignRequest))
    }

  def register(registrationRequest: PeerRegistrationRequest): PeerResponse[F, Unit] =
    PeerResponse[F, Boolean]("register", POST)(client) { (req, c) =>
      c.successful(req.withEntity(registrationRequest))
    }.flatMapF(a => if (a) F.unit else F.raiseError(new Throwable("Cannot register peer")))

  def getRegistrationRequest(): PeerResponse[F, PeerRegistrationRequest] =
    PeerResponse[F, PeerRegistrationRequest]("registration/request")(client)

}

object SignClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): SignClientInterpreter[F] =
    new SignClientInterpreter[F](client)
}
