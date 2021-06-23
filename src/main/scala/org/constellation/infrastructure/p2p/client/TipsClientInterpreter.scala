package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import io.circe.Decoder
import io.circe.generic.semiauto._
import org.constellation.schema.checkpoint.TipData._
import org.constellation.domain.p2p.client.TipsClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.checkpoint.TipData
import org.constellation.schema.{Height, Id}
import org.constellation.session.SessionTokenService
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class TipsClientInterpreter[F[_]: Concurrent: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
) extends TipsClientAlgebra[F] {

  implicit val tipDataMapDecoder: Decoder[Map[String, TipData]] = Decoder.decodeMap[String, TipData]
  implicit val idLongDecoder: Decoder[(Id, Long)] = deriveDecoder[(Id, Long)]

  def getTips(): PeerResponse[F, Set[(String, Height)]] =
    PeerResponse[F, Set[(String, Height)]]("tips")(client, sessionTokenService)

  def getHeights(): PeerResponse[F, List[Height]] =
    PeerResponse[F, List[Height]]("heights")(client, sessionTokenService)

  def getMinTipHeight(): PeerResponse[F, (Id, Long)] =
    PeerResponse[F, (Id, Long)]("heights/min")(client, sessionTokenService)
}

object TipsClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): TipsClientInterpreter[F] =
    new TipsClientInterpreter[F](client, sessionTokenService)
}
