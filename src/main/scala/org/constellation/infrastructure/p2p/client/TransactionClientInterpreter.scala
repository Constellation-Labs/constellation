package org.constellation.infrastructure.p2p.client

import cats.effect.Concurrent
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.http4s.client.Client
import io.circe.generic.auto._
import org.constellation.domain.p2p.client.TransactionClientAlgebra
import org.constellation.primitives.TransactionCacheData
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.Method._

class TransactionClientInterpreter[F[_]: Concurrent](client: Client[F]) extends TransactionClientAlgebra[F] {

  def getTransaction(hash: String): PeerResponse[F, Option[TransactionCacheData]] =
    PeerResponse(s"transaction/$hash")(client)

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[TransactionCacheData])]] =
    PeerResponse("transaction", POST) { req =>
      client.expect[List[(String, Option[TransactionCacheData])]](req.withEntity(hashes))
    }
}

object TransactionClientInterpreter {

  def apply[F[_]: Concurrent](client: Client[F]): TransactionClientInterpreter[F] =
    new TransactionClientInterpreter[F](client)
}
