package org.constellation.storage.transactions

import cats.effect.IO
import cats.implicits._
import org.constellation.primitives.{PeerData, Transaction, TransactionCacheData}
import org.constellation.storage.TransactionService
import org.constellation.{DAO, Fixtures}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSuite, Matchers}

import scala.util.Random

class TransactionGossipingTest
    extends FunSuite
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar {

  test("it should randomly select the diff of all peer IDs and peers in the tx path") {
    val dao = mockDAO
    val gossiping = new TransactionGossiping[IO](mockTxService, dao)

    val tx = mock[TransactionCacheData]
    val path = Set(Fixtures.id2, Fixtures.id3)
    tx.path shouldReturn path

    Random.setSeed(57L)

    val selectedPeers = gossiping.selectPeers(tx, 2)(Random).unsafeRunSync

    selectedPeers shouldBe Set(Fixtures.id1, Fixtures.id4)
  }

  test("it should update the transaction path if it's the first time the tx is being observed") {
    val dao = mockDAO
    val txService = mockTxService
    val gossiping = new TransactionGossiping[IO](txService, dao)

    val tx = mock[TransactionCacheData]

    tx.transaction shouldReturn mock[Transaction]
    tx.transaction.hash shouldReturn "abc"

    val path = Set(Fixtures.id2, Fixtures.id3)
    tx.path shouldReturn path

    txService.contains(tx.transaction.hash) shouldReturnF false
    txService.update(*, *) shouldReturnF Unit

    gossiping.observe(tx).unsafeRunSync

    txService.update(tx.transaction.hash, *).was(called)
  }

  test("it should not update the transaction path if it's not the first time the tx is being observed") {
    val dao = mockDAO
    val txService = mockTxService
    val gossiping = new TransactionGossiping[IO](txService, dao)

    val tx = mock[TransactionCacheData]

    tx.transaction shouldReturn mock[Transaction]
    tx.transaction.hash shouldReturn "abc"

    val path = Set(Fixtures.id2, Fixtures.id3)
    tx.path shouldReturn path

    txService.contains(tx.transaction.hash) shouldReturnF true
    txService.update(*, *) shouldReturnF Unit

    gossiping.observe(tx).unsafeRunSync

    txService.update(tx.transaction.hash, *).wasNever(called)
  }

  private def mockDAO: DAO = {
    val dao = mock[DAO]

    val peerA = Fixtures.id1 -> mock[PeerData]
    val peerB = Fixtures.id2 -> mock[PeerData]
    val peerC = Fixtures.id3 -> mock[PeerData]
    val peerD = Fixtures.id4 -> mock[PeerData]

    dao.peerInfo shouldReturn Map(peerA, peerB, peerC, peerD).pure[IO]

    dao
  }

  private def mockTxService: TransactionService[IO] = mock[TransactionService[IO]]

}
