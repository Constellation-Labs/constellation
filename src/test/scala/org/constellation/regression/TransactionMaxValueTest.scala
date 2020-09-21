package org.constellation.regression

import java.security.KeyPair

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.syntax.all._
import org.constellation.checkpoint.CheckpointBlockValidator
import org.constellation.domain.transaction.{TransactionChainService, TransactionValidator}
import org.constellation.genesis.Genesis
import org.constellation.keytool.KeyUtils
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

// TODO: Consider moving to transaction validation test suite
class TransactionMaxValueTest
    extends AnyFreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar {

  def go()(implicit dao: DAO): GenesisObservation = Genesis.createGenesisObservation(Seq.empty)

  def startingTips(go: GenesisObservation)(implicit dao: DAO): Seq[SignedObservationEdge] =
    Seq(go.initialDistribution.soe, go.initialDistribution2.soe)

  "#994" - {
    "transaction with overflow" - {
      implicit val dao = TestHelpers.prepareMockedDAO()

      implicit val keyPair = Fixtures.tempKey1
      val src = KeyUtils.publicKeyToAddressString(keyPair.getPublic)
      val dst = KeyUtils.publicKeyToAddressString(Fixtures.tempKey2.getPublic)
      val dst2 = KeyUtils.publicKeyToAddressString(Fixtures.tempKey3.getPublic)
      val dst3 = KeyUtils.publicKeyToAddressString(Fixtures.tempKey4.getPublic)

      dao.transactionService.isAccepted(*) shouldReturnF false
      val transactionChainService = mock[TransactionChainService[IO]]
      transactionChainService.getLastAcceptedTransactionRef(*) shouldReturnF LastTransactionRef.empty
      dao.transactionService.transactionChainService shouldReturn transactionChainService
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

      "when address balance is unknown" - {
        "should not pass validation" in {
          val txs = Seq(
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst, Long.MaxValue, keyPair)
          )

          as.lookup(*) shouldReturnF None

          val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips(go()))

          val isValid = cbv.simpleValidation(cb).map(_.isValid)

          isValid.unsafeRunSync shouldBe false
        }
      }
      "when address balance is known" - {
        "should not pass validation" in {
          val txs = Seq(
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst, Long.MaxValue, keyPair)
          )

          as.lookup(*) shouldReturnF AddressCacheData(20L, 20L, balanceByLatestSnapshot = 20L).some

          val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips(go()))

          val isValid = cbv.simpleValidation(cb).map(_.isValid)

          isValid.unsafeRunSync shouldBe false
        }
      }

      "when many overflowing transactions are sent" - {
        "should not pass validation with single overflow" in {
          val txs = Seq(
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst2, Long.MaxValue, keyPair),
            Fixtures.makeTransaction(src, dst3, Long.MaxValue, keyPair)
          )
          as.lookup(*) shouldReturnF AddressCacheData(20L, 20L, balanceByLatestSnapshot = 20L).some
          val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips(go()))
          val isValid = cbv.simpleValidation(cb).map(_.isValid)
          isValid.unsafeRunSync shouldBe false
        }

        "should not pass validation without single overflow" in {
          val txs = Seq(
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst, 10L, keyPair),
            Fixtures.makeTransaction(src, dst2, Long.MaxValue - 1, keyPair),
            Fixtures.makeTransaction(src, dst3, Long.MaxValue - 1, keyPair)
          )
          as.lookup(*) shouldReturnF AddressCacheData(20L, 20L, balanceByLatestSnapshot = 20L).some
          val cb = CheckpointBlock.createCheckpointBlockSOE(txs, startingTips(go()))
          val isValid = cbv.simpleValidation(cb).map(_.isValid)
          isValid.unsafeRunSync shouldBe false
        }
      }
    }
  }
}
