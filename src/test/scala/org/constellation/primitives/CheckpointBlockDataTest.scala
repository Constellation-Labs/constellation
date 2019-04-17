package org.constellation.primitives
import java.security.KeyPair
import java.util.concurrent.Executors

import better.files.File
import constellation._
import org.constellation.crypto.KeyUtils.makeKeyPair
import org.constellation.primitives.Schema.{
  CheckpointCacheData,
  CheckpointCacheFullData,
  Height,
  SignedObservationEdge
}
import org.constellation.primitives.storage._
import org.constellation.util.SignatureBatch
import org.constellation.{DAO, PeerMetadata}
import org.mockito.integrations.scalatest.IdiomaticMockitoFixture
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.ExecutionContext

class CheckpointBlockDataTest extends FunSuite with IdiomaticMockitoFixture with Matchers {

  val facilitatorId1 = Schema.Id("b")
  val peerData1 = mock[PeerData]
  peerData1.peerMetadata shouldReturn mock[PeerMetadata]
  peerData1.peerMetadata.id shouldReturn facilitatorId1
  peerData1.notification shouldReturn Seq()

  val facilitatorId2 = Schema.Id("c")
  val peerData2 = mock[PeerData]
  peerData2.peerMetadata shouldReturn mock[PeerMetadata]
  peerData2.peerMetadata.id shouldReturn facilitatorId2
  peerData2.notification shouldReturn Seq()

  val readyFacilitators = Map(facilitatorId1 -> peerData1, facilitatorId2 -> peerData2)

  val soe = mock[SignedObservationEdge]
  soe.baseHash shouldReturn "abc"

  test("should convert CB to merkle roots while storing") {

    implicit val kp: KeyPair = makeKeyPair()

    val msg1 = mock[ChannelMessage]
    msg1.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
    msg1.signedMessageData.hash shouldReturn "msg1"

    val msg2 = mock[ChannelMessage]
    msg2.signedMessageData shouldReturn mock[SignedData[ChannelMessageData]]
    msg2.signedMessageData.hash shouldReturn "msg2"

    val tx1 = mock[Transaction]
    tx1.hash shouldReturn "tx1"
    val tx2 = mock[Transaction]
    tx2.hash shouldReturn "tx2"
    val cbProposal = CheckpointBlockFullData.createCheckpointBlockSOE(Seq(tx1, tx2),
                                                                      Seq(soe),
                                                                      Seq(msg1, msg2),
                                                                      Seq.empty)

    implicit val dao: DAO = mock[DAO]

    val f = File(s"tmp/${kp.getPublic.toId.medium}/db")
    f.createDirectoryIfNotExists()
    dao.dbPath shouldReturn f
    val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))
    dao.edgeExecutionContext shouldReturn ec

    val cs = new CheckpointService(dao)
    dao.checkpointService shouldReturn cs
    val ts = new TransactionService(dao)
    ts.memPool.putSync(tx1.hash, TransactionCacheData(tx1))
    ts.memPool.putSync(tx2.hash, TransactionCacheData(tx2))

    dao.transactionService shouldReturn ts

    val ms = new MessageService()
    dao.messageService shouldReturn ms

    ms.memPool.putSync(msg1.signedMessageData.hash, ChannelMessageMetadata(msg1))
    ms.memPool.putSync(msg2.signedMessageData.hash, ChannelMessageMetadata(msg2))
    val ns = new NotificationService()
    dao.notificationService shouldReturn ns

    val ss = new SOEService()
    dao.soeService shouldReturn ss
    val recentBlockTracker = new RecentDataTracker[CheckpointCacheData](200)
    dao.recentBlockTracker shouldReturn recentBlockTracker

    val cbProposalCache = CheckpointCacheFullData(Some(cbProposal), 3, Some(Height(2, 4)))
    cbProposal.store(cbProposalCache)
    val storedCB = dao.checkpointService.memPool.getSync(cbProposal.baseHash).get
    storedCB.height shouldBe Some(Height(2, 4))
    storedCB.children shouldBe 3

    CheckpointService.fetchFullData(storedCB) shouldBe cbProposalCache

  }

}
