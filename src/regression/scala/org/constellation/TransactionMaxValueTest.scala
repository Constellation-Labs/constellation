package org.constellation

import java.security.KeyPair

import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.implicits._
import org.constellation.checkpoint.CheckpointBlockValidator
import org.constellation.domain.transaction.TransactionValidator
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.{CheckpointBlock, Genesis}
import org.constellation.primitives.Schema.{AddressCacheData, GenesisObservation, SignedObservationEdge}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{FreeSpec, Matchers}

class TransactionMaxValueTest
    extends FreeSpec
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
    }
  }
}
