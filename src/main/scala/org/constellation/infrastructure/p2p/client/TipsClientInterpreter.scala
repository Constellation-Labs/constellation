package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.consensus.TipData
import org.constellation.domain.p2p.client.TipsClientAlgebra
import org.constellation.primitives.Schema.Height
import org.constellation.schema.Id
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

class TipsClientInterpreter[F[_]: Concurrent: ContextShift](client: Client[F]) extends TipsClientAlgebra[F] {

  def getTips(): PeerResponse[F, Map[String, TipData]] =
    PeerResponse[F, Map[String, TipData]]("tips")(client)

  def getHeights(): PeerResponse[F, List[Height]] =
    PeerResponse[F, List[Height]]("heights")(client)

  def getMinTipHeight(): PeerResponse[F, (Id, Long)] =
    PeerResponse[F, (Id, Long)]("heights/min")(client)
}

object TipsClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](client: Client[F]): TipsClientInterpreter[F] =
    new TipsClientInterpreter[F](client)
}
