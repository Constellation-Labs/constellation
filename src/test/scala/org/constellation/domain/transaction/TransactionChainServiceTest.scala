package org.constellation.domain.transaction

import java.security.KeyPair

import cats.effect.{Concurrent, IO}
import cats.implicits._
import constellation.signedObservationEdge
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.primitives.Schema.{EdgeHashType, ObservationEdge, TransactionEdgeData, TypedEdgeHash}
import org.constellation.primitives.{Edge, Schema, Transaction}
import org.constellation.{ConstellationExecutionContext, Fixtures}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class TransactionChainServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val cs = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
  var service: TransactionChainService[IO] = _

  before {
    service = TransactionChainService[IO]
  }

  "getLastTransactionRef" - {
    "should return empty string and 0 ordinal if address is unknown" in {
      val last = service.getLastTransactionRef("unknown")

      last.unsafeRunSync shouldBe LastTransactionRef("", 0L)
    }

    "should return last transaction ref if address is known" in {}
  }

  "createAndSetLastTransaction" - {
    "should return new transaction" in {
      val tx = createTransaction("unknown", "bb").unsafeRunSync
      val createdTx = service.setLastTransaction(tx.edge, false).unsafeRunSync

      createdTx shouldBe tx
    }
  }

  def createTransaction(
    src: String,
    dst: String
  ): IO[Transaction] = {
    val txData = TransactionEdgeData(1L, LastTransactionRef("", 0L))

    val oe = ObservationEdge(
      Seq(TypedEdgeHash(src, EdgeHashType.AddressHash), TypedEdgeHash(dst, EdgeHashType.AddressHash)),
      TypedEdgeHash(txData.hash, EdgeHashType.TransactionDataHash)
    )

    val soe = signedObservationEdge(oe)(Fixtures.tempKey)

    for {
      last <- service.getLastTransactionRef(src)
      tx = Transaction(Edge(oe, soe, txData), LastTransactionRef(last.hash, last.ordinal))
    } yield tx
  }

}
