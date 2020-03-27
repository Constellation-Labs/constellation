package org.constellation

import java.security.KeyPair

import cats.effect.IO
import cats.implicits._
import cats.effect.concurrent.Ref
import org.constellation.checkpoint.CheckpointBlockValidator
import org.constellation.domain.transaction.TransactionValidator
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.{CheckpointBlock, Genesis, TransactionCacheData}
import org.constellation.primitives.Schema.{AddressCacheData, GenesisObservation, SignedObservationEdge}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{FreeSpec, Matchers}

class DoubleSpendValidationTest
    extends FreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar {

  def go()(implicit dao: DAO): GenesisObservation = Genesis.createGenesisObservation(Seq.empty)

  def startingTips(go: GenesisObservation)(implicit dao: DAO): Seq[SignedObservationEdge] =
    Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

  "#1007" - {
    implicit val dao = TestHelpers.prepareMockedDAO()

    implicit val keyPair = Fixtures.tempKey1
    val src = KeyUtils.publicKeyToAddressString(keyPair.getPublic)
    val dst = KeyUtils.publicKeyToAddressString(Fixtures.tempKey2.getPublic)

    dao.transactionService.isAccepted(*) shouldReturnF false
    dao.transactionService.createDummyTransaction(*, *, *) shouldAnswer { (a: String, b: String, c: KeyPair) =>
      IO { Fixtures.makeDummyTransaction(a, b, c) }
    }

    val as = dao.addressService
    val ss = dao.snapshotService
    val cps = dao.checkpointParentService
    val tv = new TransactionValidator[IO](dao.transactionService)
    val cbv = new CheckpointBlockValidator[IO](
      as,
      ss,
      cps,
      tv,
      dao
    )

    ss.lastSnapshotHeight shouldReturn Ref.unsafe[IO, Int](0)

    "when a double spend is encountered" - {
      "the transaction with double spend should be invalid" in {
        val alreadyAcceptedTx = Fixtures.makeTransaction(src, dst, 10L, keyPair)

        as.lookup(*) shouldReturnF AddressCacheData(200L, 200L, balanceByLatestSnapshot = 200L).some
        dao.transactionService.isAccepted(alreadyAcceptedTx.hash) shouldReturnF true

        val txValidator = new TransactionValidator[IO](dao.transactionService)

        val isValidTx = txValidator.validateTransaction(alreadyAcceptedTx).map(_.isValid)

        isValidTx.unsafeRunSync shouldBe false
      }

      "the checkpoint block with with double spend transaction should be invalid" in {
        val alreadyAcceptedTx = Fixtures.makeTransaction(src, dst, 10L, keyPair)

        val txs = Seq(
          alreadyAcceptedTx,
          Fixtures.makeTransaction(src, dst, 11L, keyPair),
          Fixtures.makeTransaction(src, dst, 12L, keyPair)
        )

        as.lookup(*) shouldReturnF AddressCacheData(200L, 200L, balanceByLatestSnapshot = 200L).some
        dao.transactionService.isAccepted(alreadyAcceptedTx.hash) shouldReturnF true

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips(go()))

        val isValidCb = cbv.simpleValidation(cb).map(_.isValid)

        isValidCb.unsafeRunSync shouldBe false
      }
    }
  }
}
