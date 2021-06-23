package org.constellation.infrastructure.endpoints

import cats.effect.{ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.constellation.checkpoint.CheckpointBlockValidator
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.schema.transaction.LastTransactionRef
import org.constellation.util.Metrics
import org.http4s.implicits._
import org.http4s.{HttpRoutes, Method, Request, Status}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
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
  var checkpointBlockValidator: CheckpointBlockValidator[IO] = _
  var transactionPeerEndpoints: HttpRoutes[IO] = _
  var transactionPublicEndpoints: HttpRoutes[IO] = _

  before {
    transactionChainService = mock[TransactionChainService[IO]]
    transactionService = mock[TransactionService[IO]]
    metrics = mock[Metrics]
    checkpointBlockValidator = mock[CheckpointBlockValidator[IO]]

    transactionPeerEndpoints = TransactionEndpoints.peerEndpoints[IO](transactionService, metrics)
    transactionPublicEndpoints = TransactionEndpoints.publicEndpoints[IO](transactionService, checkpointBlockValidator)
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

    "findTransactionsEndpoint" - {
      "should respond with an Ok status" in {
        transactionService.findByPredicate(*) shouldReturnF List.empty

        val response =
          transactionPublicEndpoints.orNotFound.run(Request(Method.GET, uri"transaction?src=src")).unsafeRunSync

        response.status shouldBe Status.Ok
      }
    }
  }
}
