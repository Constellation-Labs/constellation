package org.constellation.regression

import java.security.KeyPair

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.syntax.all._
import org.constellation.checkpoint.CheckpointBlockValidator
import org.constellation.domain.transaction.{TransactionChainService, TransactionValidator}
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Genesis
import org.constellation.schema.GenesisObservation
import org.constellation.schema.address.AddressCacheData
import org.constellation.schema.checkpoint.CheckpointBlock
import org.constellation.schema.edge.SignedObservationEdge
import org.constellation.schema.transaction.LastTransactionRef
import org.constellation.{DAO, Fixtures, TestHelpers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

// TODO: Consider moving to validation test suite
class DoubleSpendValidationTest
    extends AnyFreeSpec
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

    val transactionChainService = mock[TransactionChainService[IO]]
    transactionChainService.getLastAcceptedTransactionRef(*) shouldReturnF LastTransactionRef.empty
    dao.transactionService.transactionChainService shouldReturn transactionChainService
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

      "the checkpoint block with dummy transaction should be still valid" in {
        val txs = Seq(
          Fixtures.makeDummyTransaction(src, dst, keyPair),
          Fixtures.makeDummyTransaction(src, dst, keyPair),
          Fixtures.makeDummyTransaction(src, dst, keyPair)
        )

        as.lookup(*) shouldReturnF None
        dao.transactionService.isAccepted(*) shouldReturnF false

        val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips(go()))

        val isValidCb = cbv.simpleValidation(cb).map(_.isValid)
        isValidCb.unsafeRunSync shouldBe true

        val isValidTx = cbv.singleTransactionValidation(txs.head).map(_.isValid)
        isValidTx.unsafeRunSync shouldBe true
      }
    }
  }
}
