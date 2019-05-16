package org.constellation.primitives.storage

import java.security.KeyPair
import java.util.concurrent.Executors

import better.files.File
import constellation._
import org.constellation.crypto.KeyUtils.makeKeyPair
import org.constellation.primitives.Schema.{CheckpointCache, Height, SignedObservationEdge}
import org.constellation.primitives._
import org.constellation.util.Metrics
import org.constellation.{DAO, PeerMetadata}
import org.mockito.Mockito.doNothing
import org.mockito.integrations.scalatest.IdiomaticMockitoFixture
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.ExecutionContext

class CheckpointServiceTest
    extends FunSuite
    with IdiomaticMockitoFixture
    with Matchers
    with BeforeAndAfter {

  val readyFacilitators: Map[Schema.Id, PeerData] = prepareFacilitators()
  val soe: SignedObservationEdge = mock[SignedObservationEdge]

  implicit val kp: KeyPair = makeKeyPair()
  implicit val dao: DAO = prepareDao()

  before {
    soe.baseHash shouldReturn "abc"
  }

  test("should convert CB to merkle roots when all data is filled") {

    val cbProposal = CheckpointBlock.createCheckpointBlockSOE(prepareTransactions(),
                                                              Seq(soe),
                                                              prepareMessages(),
                                                              prepareNotifications())

    val cbProposalCache = CheckpointCache(Some(cbProposal), 3, Some(Height(2, 4)))
    dao.checkpointService.memPool.putSync(cbProposal.baseHash, cbProposalCache)

    val storedCB = dao.checkpointService.memPool.getSync(cbProposal.baseHash).get

    CheckpointService.convert(storedCB) shouldBe cbProposalCache
  }

  test("should convert CB to merkle roots when minimum data is filled") {
    val fullData = storeCheckpointBlock(prepareTransactions(), Seq.empty, Seq.empty)
    val storedCB = dao.checkpointService.memPool.getSync(fullData.checkpointBlock.get.baseHash).get

    CheckpointService.convert(storedCB) shouldBe fullData
  }

  test("should fetch messages when they exist") {
    val msgs = prepareMessages()
    val fullData = storeCheckpointBlock(prepareTransactions(), msgs, Seq.empty)
    val storedCB = dao.checkpointService.memPool.getSync(fullData.checkpointBlock.get.baseHash).get

    CheckpointService
      .fetchMessages(storedCB.checkpointBlock.messagesMerkleRoot.get) shouldBe msgs
  }

  test("should fetch transactions when they exist") {
    val txs = prepareTransactions()
    val fullData = storeCheckpointBlock(txs, Seq.empty, Seq.empty)
    val storedCB = dao.checkpointService.memPool.getSync(fullData.checkpointBlock.get.baseHash).get

    CheckpointService
      .fetchTransactions(storedCB.checkpointBlock.transactionsMerkleRoot) shouldBe txs
  }

  test("should fetch notifications when they exist") {
    val notifications = prepareNotifications()
    val fullData = storeCheckpointBlock(prepareTransactions(), Seq.empty, notifications)
    val storedCB = dao.checkpointService.memPool.getSync(fullData.checkpointBlock.get.baseHash).get

    CheckpointService
      .fetchNotifications(storedCB.checkpointBlock.notificationsMerkleRoot.get) shouldBe notifications
  }

  private def storeCheckpointBlock(txs: Seq[Transaction],
                                   msgs: Seq[ChannelMessage],
                                   notifics: Seq[PeerNotification]): CheckpointCache = {

    val cbProposal = CheckpointBlock.createCheckpointBlockSOE(txs, Seq(soe), msgs, notifics)

    val cbProposalCache = CheckpointCache(Some(cbProposal), 3, Some(Height(2, 4)))
    dao.checkpointService.memPool.putSync(cbProposal.baseHash, cbProposalCache)
    cbProposalCache
  }

  private def prepareTransactions(): Seq[Transaction] = {
    val tx1 = mock[Transaction]
    tx1.hash shouldReturn "tx1"
    val tx2 = mock[Transaction]
    tx2.hash shouldReturn "tx2"
    dao.transactionService.memPool.putSync(tx1.hash, TransactionCacheData(tx1))
    dao.transactionService.memPool.putSync(tx2.hash, TransactionCacheData(tx2))
    Seq(tx1, tx2)
  }

  private def prepareNotifications(): Seq[PeerNotification] = {
    val notification1 = mock[PeerNotification]
    notification1.hash shouldReturn "notification1"

    val notification2 = mock[PeerNotification]
    notification2.hash shouldReturn "notification2"

    dao.notificationService.memPool.putSync(notification1.hash, notification1)
    dao.notificationService.memPool.putSync(notification2.hash, notification2)
    Seq(notification1, notification2)
  }

  private def prepareMessages(): Seq[ChannelMessage] = {
    val msg1 = mock[ChannelMessage]
    msg1.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
    msg1.signedMessageData.hash shouldReturn "msg1"

    val msg2 = mock[ChannelMessage]
    msg2.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
    msg2.signedMessageData.hash shouldReturn "msg2"

    dao.messageService.memPool
      .putSync(msg1.signedMessageData.hash, ChannelMessageMetadata(msg1))
    dao.messageService.memPool
      .putSync(msg2.signedMessageData.hash, ChannelMessageMetadata(msg2))
    Seq(msg1, msg2)
  }

  private def prepareDao(): DAO = {
    val dao: DAO = mock[DAO]
    val f = File(s"tmp/${kp.getPublic.toId.medium}/db")
    f.createDirectoryIfNotExists()
    dao.dbPath shouldReturn f

    val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))
    dao.edgeExecutionContext shouldReturn ec
    val cs = new CheckpointService(dao)
    dao.checkpointService shouldReturn cs

    val ss = new SOEService()
    dao.soeService shouldReturn ss

    val ns = new NotificationService()
    dao.notificationService shouldReturn ns

    val ms = new MessageService()(dao)
    dao.messageService shouldReturn ms

    val ts = new TransactionService(dao)
    dao.transactionService shouldReturn ts

    val metrics = mock[Metrics]
    doNothing().when(metrics).incrementMetric(*)
    dao.metrics shouldReturn metrics

    dao
  }

  private def prepareFacilitators(): Map[Schema.Id, PeerData] = {
    val facilitatorId1 = Schema.Id("b")
    val peerData1: PeerData = mock[PeerData]
    peerData1.peerMetadata shouldReturn mock[PeerMetadata]
    peerData1.peerMetadata.id shouldReturn facilitatorId1
    peerData1.notification shouldReturn Seq()

    val facilitatorId2 = Schema.Id("c")
    val peerData2: PeerData = mock[PeerData]
    peerData2.peerMetadata shouldReturn mock[PeerMetadata]
    peerData2.peerMetadata.id shouldReturn facilitatorId2
    peerData2.notification shouldReturn Seq()

    Map(facilitatorId1 -> peerData1, facilitatorId2 -> peerData2)
  }
}
