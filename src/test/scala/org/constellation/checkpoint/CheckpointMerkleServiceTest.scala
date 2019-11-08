package org.constellation.checkpoint

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.domain.consensus.ConsensusStatus
import org.constellation.p2p.PeerNotification
import org.constellation.primitives.Schema.{CheckpointCache, Height, SignedObservationEdge}
import org.constellation.primitives._
import org.constellation.schema.{ChannelMessageData, HashGenerator}
import org.constellation.serializer.KryoHashGenerator
import org.constellation.{ConstellationExecutionContext, DAO, TestHelpers}
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class CheckpointMerkleServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with Matchers
    with BeforeAndAfter {

  implicit val dao: DAO = TestHelpers.prepareRealDao()
  implicit val hashGenerator: HashGenerator = new KryoHashGenerator
  val soe: SignedObservationEdge = mock[SignedObservationEdge]

  before {
    soe.baseHash shouldReturn "abc"
  }

  "with mocked dao" - {
    implicit val context: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

    val merkleService = new CheckpointMerkleService[IO](
      dao,
      hashGenerator,
      dao.transactionService,
      dao.messageService,
      dao.notificationService,
      dao.observationService
    )
    "should convert CB to merkle roots when all data is filled" in {

      val cbProposal = CheckpointBlock
        .createCheckpointBlockSOE(prepareTransactions(), Seq(soe), prepareMessages(), prepareNotifications())(
          dao.keyPair,
          hashGenerator
        )

      val cbProposalCache = CheckpointCache(Some(cbProposal), 3, Some(Height(2, 4)))

      dao.checkpointService.put(cbProposalCache).unsafeRunSync()

      val storedCB = dao.checkpointService.memPool.lookup(cbProposal.baseHash).unsafeRunSync().get

      merkleService.convert(storedCB).unsafeRunSync() shouldBe cbProposalCache
    }

    "should convert CB to merkle roots when minimum data is filled" in {
      val fullData = storeCheckpointBlock(prepareTransactions(), Seq.empty, Seq.empty)
      val storedCB = dao.checkpointService.memPool.lookup(fullData.checkpointBlock.get.baseHash).unsafeRunSync().get

      merkleService.convert(storedCB).unsafeRunSync() shouldBe fullData
    }

    "should fetch messages when they exist" in {
      val msgs = prepareMessages()
      val fullData = storeCheckpointBlock(prepareTransactions(), msgs, Seq.empty)
      val storedCB = dao.checkpointService.memPool.lookup(fullData.checkpointBlock.get.baseHash).unsafeRunSync().get

      merkleService
        .fetchMessages(storedCB.checkpointBlock.messagesMerkleRoot.get)
        .unsafeRunSync() shouldBe msgs
    }

    "should fetch transactions when they exist" in {
      val txs = prepareTransactions()
      val fullData = storeCheckpointBlock(txs, Seq.empty, Seq.empty)
      val storedCB = dao.checkpointService.memPool.lookup(fullData.checkpointBlock.get.baseHash).unsafeRunSync().get

      dao.checkpointService
        .fetchBatchTransactions(storedCB.checkpointBlock.transactionsMerkleRoot.get)
        .unsafeRunSync() shouldBe txs
    }

    "should fetch notifications when they exist" in {
      val notifications = prepareNotifications()
      val fullData = storeCheckpointBlock(prepareTransactions(), Seq.empty, notifications)
      val storedCB = dao.checkpointService.memPool.lookup(fullData.checkpointBlock.get.baseHash).unsafeRunSync().get

      merkleService
        .fetchNotifications(storedCB.checkpointBlock.notificationsMerkleRoot.get)
        .unsafeRunSync() shouldBe notifications
    }
  }
  private def storeCheckpointBlock(
    txs: Seq[Transaction],
    msgs: Seq[ChannelMessage],
    notifics: Seq[PeerNotification]
  ): CheckpointCache = {

    val cbProposal = CheckpointBlock.createCheckpointBlockSOE(txs, Seq(soe), msgs, notifics)(dao.keyPair, hashGenerator)

    val cbProposalCache = CheckpointCache(Some(cbProposal), 3, Some(Height(2, 4)))
    dao.checkpointService.put(cbProposalCache).unsafeRunSync()
    cbProposalCache
  }

  private def prepareTransactions(): Seq[Transaction] = {
    val tx1 = mock[Transaction]
    tx1.hash shouldReturn "tx1"
    val tx2 = mock[Transaction]
    tx2.hash shouldReturn "tx2"

    (dao.transactionService.put(TransactionCacheData(tx1), ConsensusStatus.Accepted) >>
      dao.transactionService.put(TransactionCacheData(tx2), ConsensusStatus.Accepted))
      .unsafeRunSync()

    Seq(tx1, tx2)
  }

  private def prepareNotifications(): Seq[PeerNotification] = {
    val notification1 = mock[PeerNotification]
    notification1.hash shouldReturn "notification1"

    val notification2 = mock[PeerNotification]
    notification2.hash shouldReturn "notification2"

    (dao.notificationService.memPool.put(notification1.hash, notification1) >>
      dao.notificationService.memPool.put(notification2.hash, notification2))
      .unsafeRunSync()

    Seq(notification1, notification2)
  }

  private def prepareMessages(): Seq[ChannelMessage] = {
    val msg1 = mock[ChannelMessage]
    msg1.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
    msg1.signedMessageData.hash shouldReturn "msg1"

    val msg2 = mock[ChannelMessage]
    msg2.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
    msg2.signedMessageData.hash shouldReturn "msg2"

    (dao.messageService.memPool.put(msg1.signedMessageData.hash, ChannelMessageMetadata(msg1)) >>
      dao.messageService.memPool.put(msg2.signedMessageData.hash, ChannelMessageMetadata(msg2)))
      .unsafeRunSync()
    Seq(msg1, msg2)
  }
}
