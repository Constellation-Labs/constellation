package org.constellation

import java.security.KeyPair

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.syntax.all._
import com.typesafe.scalalogging.StrictLogging
import fs2.{Pipe, Stream}
import io.circe.syntax._
import org.constellation.testutils.HttpUtil.evalRequestForHosts
import org.constellation.keytool.KeyStoreUtils
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.serialization.Kryo
import org.constellation.schema.transaction.{LastTransactionRef, Transaction}
import org.constellation.testutils.CustomMatchers
import org.constellation.wallet.{TransactionExt, Wallet}
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.client.dsl.io._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, Request, Uri}
import org.mockito.IdiomaticMockito
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.scalatest.concurrent.Eventually
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar._
import org.scalatest.tagobjects.Slow

import scala.concurrent.ExecutionContext.global

class TransactionsProcessingSpec
    extends AnyFreeSpec
    with StrictLogging
    with GivenWhenThen
    with CustomMatchers
    with TestConfig
    with TerraformOutput
    with Eventually
    with BeforeAndAfterAll
    with IdiomaticMockito {

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(2 minutes),
      interval = scaled(5 seconds)
    )

  override protected def beforeAll(): Unit =
    Kryo.init[IO]().unsafeRunSync()

  "sending transactions from single src address" in {
    // given
    val kp = getKeyPair
    val (src, dst) = (Wallet.getAddress(kp), config.txGen.dst)

    val srcBalances = getBalances(src)
    srcBalances should containTheSameElements

    val dstBalances = getBalances(dst)
    dstBalances should containTheSameElements
    logger.info(
      s"Initial balances: src address = $src, src balance = ${srcBalances.head}; dst address = $dst, dst balance = ${dstBalances.head}"
    )

    val lastTxRefs = getLasTxRefs(src)
    lastTxRefs should containTheSameElements

    // when
    val txs = generateAndSendTxs(kp, lastTxRefs.head)
    logger.info(s"Sent transactions $txs")

    // then
    val targetSrcBalance = srcBalances.head - (config.txGen.amount + config.txGen.fee) * config.txGen.countTotal
    val targetDstBalance = dstBalances.head + config.txGen.amount * config.txGen.countTotal

    eventually {
      getBalances(src) should contain only targetSrcBalance
      getBalances(dst) should contain only targetDstBalance
    }
  }

  private def getKeyPair =
    KeyStoreUtils
      .keyPairFromStorePath[IO](
        config.txGen.keyPair.keystore,
        config.txGen.keyPair.alias,
        config.txGen.keyPair.storepass.toCharArray,
        config.txGen.keyPair.keypass.toCharArray
      )
      .value
      .flatMap {
        case Right(value) => value.pure[IO]
        case Left(err)    => err.raiseError[IO, KeyPair]
      }
      .unsafeRunSync()

  private def generateAndSendTxs(kp: KeyPair, ltr: LastTransactionRef): List[String] = {
    val firstTx = mock[Transaction]
    firstTx.hash shouldReturn ltr.prevHash
    firstTx.ordinal shouldReturn ltr.ordinal

    val src = Wallet.getAddress(kp)

    def generateTxs(prevTx: Transaction): Stream[IO, Transaction] = {
      val tx = TransactionExt.createTransaction(
        prevTx.some,
        ltr,
        forcePrevTx = true,
        src,
        config.txGen.dst,
        config.txGen.amount,
        kp,
        (config.txGen.fee.toDouble * 1e-8).some.filter(_ > 0)
      )
      Stream(tx) ++ generateTxs(tx)
    }

    evalRequestForHosts[String](terraform.instanceIps.value) {
      _.flatMap(Stream(_).repeatN(config.txGen.countPerHost))
        .zipWith(generateTxs(firstTx)) { (host, tx) =>
          POST(tx.asJson, Uri.unsafeFromString(s"http://$host:9000/transaction"))
        }
        .take(config.txGen.countTotal)
    }
  }

  private def getBalances(address: String) =
    evalRequestForHosts[AddressCacheData](terraform.instanceIps.value) {
      _.map(host => GET(Uri.unsafeFromString(s"http://$host:9000/address/$address")))
    }.map(_.balance)

  private def getLasTxRefs(address: String) =
    evalRequestForHosts[LastTransactionRef](terraform.instanceIps.value) {
      _.map(host => GET(Uri.unsafeFromString(s"http://$host:9000/transaction/last-ref/$address")))
    }

}
