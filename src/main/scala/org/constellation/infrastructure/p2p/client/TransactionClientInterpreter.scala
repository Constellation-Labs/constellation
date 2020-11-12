package org.constellation.infrastructure.p2p.client

import cats.effect.{Concurrent, ContextShift}
import org.constellation.domain.p2p.client.TransactionClientAlgebra
import org.constellation.infrastructure.p2p.PeerResponse
import org.constellation.infrastructure.p2p.PeerResponse.PeerResponse
import org.constellation.schema.v2.transaction.{LastTransactionRef, TransactionCacheData}
import org.constellation.session.SessionTokenService
import org.http4s.Method._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.Client

import scala.language.reflectiveCalls

class TransactionClientInterpreter[F[_]: Concurrent: ContextShift](
  client: Client[F],
  sessionTokenService: SessionTokenService[F]
) extends TransactionClientAlgebra[F] {

  def getTransaction(hash: String): PeerResponse[F, Option[TransactionCacheData]] =
    PeerResponse(s"transaction/$hash")(client, sessionTokenService)

  def getBatch(hashes: List[String]): PeerResponse[F, List[(String, Option[TransactionCacheData])]] =
    PeerResponse("batch/transactions", POST)(client, sessionTokenService) { (req, c) =>
      c.expect[List[(String, Option[TransactionCacheData])]](req.withEntity(hashes))
    }

  def getLastTransactionRef(address: String): PeerResponse[F, LastTransactionRef] =
    PeerResponse(s"transaction/last-ref/$address")(client, sessionTokenService)
}

object TransactionClientInterpreter {

  def apply[F[_]: Concurrent: ContextShift](
    client: Client[F],
    sessionTokenService: SessionTokenService[F]
  ): TransactionClientInterpreter[F] =
    new TransactionClientInterpreter[F](client, sessionTokenService)
}
