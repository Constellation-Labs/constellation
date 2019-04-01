package org.constellation.primitives

import org.constellation.DAO
import org.constellation.primitives.CheckpointBlockValidatorNel._
import org.constellation.primitives.Schema.{CheckpointCacheData, Id}
import org.constellation.primitives.storage.CheckpointService
import org.constellation.util.HashSignature
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class CheckpointBlockValidatorNelTest extends FunSuite with Matchers with BeforeAndAfter {

  implicit val dao: DAO = mock(classOf[DAO])
  val snapService: ThreadSafeSnapshotService = Mockito.mock(classOf[ThreadSafeSnapshotService])
  val checkpointService: CheckpointService = Mockito.mock(classOf[CheckpointService])

  val leftBlock: CheckpointBlock = Mockito.mock(classOf[CheckpointBlock])
  val leftParent: CheckpointBlock = Mockito.mock(classOf[CheckpointBlock])

  val rightBlock: CheckpointBlock = Mockito.mock(classOf[CheckpointBlock])
  val rightParent: CheckpointBlock = Mockito.mock(classOf[CheckpointBlock])

  val tx1: Transaction = mock(classOf[Transaction])
  val tx2: Transaction = mock(classOf[Transaction])
  val tx3: Transaction = mock(classOf[Transaction])
  val tx4: Transaction = mock(classOf[Transaction])

  before {
    when(leftBlock.baseHash).thenReturn("block1")
    when(leftParent.baseHash).thenReturn("leftParent")
    when(rightBlock.baseHash).thenReturn("block2")
    when(rightParent.baseHash).thenReturn("rightParent")

    when(rightBlock.signatures).thenReturn(
      Seq(HashSignature.apply("sig1", Id("id1")), HashSignature.apply("sig2", Id("id2")))
    )
    when(leftBlock.signatures).thenReturn(
      Seq(HashSignature.apply("sig1", Id("id1")), HashSignature.apply("sig2", Id("id2")))
    )

    when(rightBlock.parentSOEBaseHashes()(any())).thenReturn(Seq("rightParent"))
    when(leftBlock.parentSOEBaseHashes()(any())).thenReturn(Seq("leftParent"))

    when(leftParent.parentSOEBaseHashes()(any())).thenReturn(Seq.empty)
    when(rightParent.parentSOEBaseHashes()(any())).thenReturn(Seq.empty)

    when(leftParent.transactions).thenReturn(Seq.empty)
    when(rightParent.transactions).thenReturn(Seq.empty)

    when(checkpointService.get(rightParent.baseHash))
      .thenReturn(Some(CheckpointCacheData(Some(rightParent))))
    when(checkpointService.get(leftParent.baseHash))
      .thenReturn(Some(CheckpointCacheData(Some(leftParent))))

    when(leftBlock.transactions).thenReturn(Seq(tx1, tx2))

    when(rightBlock.transactions).thenReturn(Seq(tx3, tx4))

    when(dao.threadSafeSnapshotService).thenReturn(snapService)
    when(dao.checkpointService).thenReturn(checkpointService)
    when(snapService.acceptedCBSinceSnapshot).thenReturn(Seq.empty)
  }

  test("it should detect no conflict and return None") {
    detectAncestryConflict(Seq(leftBlock, rightBlock)) shouldBe None
  }

  test("it should detect direct conflict with other tip") {
    val rightBlockTx = rightBlock.transactions.head
    when(leftBlock.transactions).thenReturn(Seq(tx1, tx2, rightBlockTx))

    detectAncestryConflict(Seq(leftBlock, rightBlock)) shouldBe Some(rightBlock)
  }

  test("it should detect conflict with ancestry of other tip") {
    when(rightParent.transactions).thenReturn(Seq(tx2))

    detectAncestryConflict(Seq(leftBlock, rightBlock)) shouldBe Some(leftBlock)
  }

  test("it should get transactions from parent") {
    when(rightParent.transactions).thenReturn(Seq(tx2))

    val combinedTxs =
      getTransactionsTillSnapshot(List(rightBlock))
    combinedTxs shouldBe rightParent.transactions
  }

  test("it should return correct block to preserve with greater base hash") {
    selectBlockToPreserve(
      Seq(CheckpointCacheData(Some(leftBlock)), CheckpointCacheData(Some(rightBlock)))
    ) shouldBe CheckpointCacheData(Some(rightBlock))
  }

  test("it should return correct block to preserve with greater number of signatures") {
    val signatures = rightBlock.signatures ++ Seq(HashSignature.apply("extraSig", Id("extra_id1")))

    when(leftBlock.signatures).thenReturn(signatures)

    selectBlockToPreserve(
      Seq(CheckpointCacheData(Some(leftBlock)), CheckpointCacheData(Some(rightBlock)))
    ) shouldBe CheckpointCacheData(Some(leftBlock))
  }

  test("it should return correct block to preserve with greater number of children") {
    selectBlockToPreserve(
      Seq(CheckpointCacheData(Some(leftBlock), 2), CheckpointCacheData(Some(rightBlock)))
    ) shouldBe CheckpointCacheData(Some(leftBlock), 2)
  }
}
