package org.constellation.storage.transactions

import cats.effect.{ContextShift, IO}
import cats.effect.concurrent.Semaphore
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.p2p.PeerData
import org.constellation.primitives.{Transaction, TransactionCacheData}
import org.constellation.storage.{ConsensusStatus, TransactionService}
import org.constellation.{ConstellationContextShift, DAO, Fixtures}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext
import scala.util.Random

class TransactionGossipingTest
    extends FunSuite
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  implicit val cs: ContextShift[IO] = ConstellationContextShift.global
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  test("it should randomly select the diff of all peer IDs and peers in the tx path") {
    val dao = mockDAO
    val gossiping = new TransactionGossiping[IO](mockTxService, 2, dao)

    val tx = mock[TransactionCacheData]
    val path = Set(Fixtures.id2, Fixtures.id3)
    tx.path shouldReturn path

    Random.setSeed(57L)

    val selectedPeers = gossiping.selectPeers(tx)(Random).unsafeRunSync

    selectedPeers shouldBe Set(Fixtures.id1, Fixtures.id4)
  }

  test("it should update the transaction path if it's the first time the tx is being observed") {
    val dao = mockDAO
    val txService = spy(new TransactionService[IO](dao))
    val gossiping = new TransactionGossiping[IO](txService, 2, dao)

    val t = constellation.createTransaction("a", "b", 1L, Fixtures.tempKey, false)
    val tx = TransactionCacheData(transaction = t, path = Set(Fixtures.id2, Fixtures.id3))

    gossiping.observe(tx).unsafeRunSync

    txService.lookup(tx.transaction.hash, ConsensusStatus.Unknown).map(_.map(_.path)).unsafeRunSync shouldBe Some(
      Set(Fixtures.id2, Fixtures.id3, dao.id)
    )
  }

  test("it should merge the transaction path if it's not the first time the tx is being observed") {
    val dao = mockDAO
    val txService = mockTxService
    val gossiping = new TransactionGossiping[IO](txService, 2, dao)

    val tx = mock[TransactionCacheData]

    tx.transaction shouldReturn mock[Transaction]
    tx.transaction.hash shouldReturn "abc"

    val path = Set(Fixtures.id2, Fixtures.id3)
    tx.path shouldReturn path

    txService.contains(tx.transaction.hash) shouldReturnF true
    txService.update(*, *) shouldReturnF Some(tx)
    txService.lookup(*) shouldReturnF Some(tx)

    gossiping.observe(tx).unsafeRunSync

    txService.update(tx.transaction.hash, *).was(called)
  }

  private def mockDAO: DAO = {
    val dao = mock[DAO]

    dao.id shouldReturn Fixtures.id5

    val peerA = Fixtures.id1 -> mock[PeerData]
    val peerB = Fixtures.id2 -> mock[PeerData]
    val peerC = Fixtures.id3 -> mock[PeerData]
    val peerD = Fixtures.id4 -> mock[PeerData]

    dao.peerInfo shouldReturn Map(peerA, peerB, peerC, peerD).pure[IO]

    dao
  }

  private def mockTxService: TransactionService[IO] = mock[TransactionService[IO]]

}
