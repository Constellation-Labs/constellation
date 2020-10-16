package org.constellation.infrastructure.endpoints

import cats.effect.{ContextShift, IO}
import io.circe.generic.auto._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.implicits._
import org.constellation.ConstellationExecutionContext
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.schema.transaction.LastTransactionRef
import org.constellation.util.Metrics
import org.http4s.{HttpRoutes, Method, Request, Status}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class TransactionEndpointsTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  var transactionChainService: TransactionChainService[IO] = _
  var transactionService: TransactionService[IO] = _
  var metrics: Metrics = _
  var transactionPeerEndpoints: HttpRoutes[IO] = _

  before {
    transactionChainService = mock[TransactionChainService[IO]]
    transactionService = mock[TransactionService[IO]]
    metrics = mock[Metrics]

    transactionPeerEndpoints = TransactionEndpoints.peerEndpoints[IO](transactionService, metrics)
  }

  "transactionPeerEndpoints" - {
    "getLastTransactionEndpoint" - {
      "should call a method returning reference to the last transaction from transactionChainService" in {
        transactionService.transactionChainService shouldReturn transactionChainService
        transactionChainService.getLastAcceptedTransactionRef(*) shouldReturnF LastTransactionRef.empty
        transactionPeerEndpoints.orNotFound.run(Request(Method.GET, uri"transaction/last-ref/DAG123")).unsafeRunSync

        transactionChainService.getLastAcceptedTransactionRef("DAG123").wasCalled(once)
      }

      "should respond with an Ok status" in {
        transactionService.transactionChainService shouldReturn transactionChainService
        transactionChainService.getLastAcceptedTransactionRef(*) shouldReturnF LastTransactionRef.empty
        val response =
          transactionPeerEndpoints.orNotFound.run(Request(Method.GET, uri"transaction/last-ref/DAG123")).unsafeRunSync

        response.status shouldBe Status.Ok
      }
    }
  }
}
