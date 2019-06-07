package org.constellation

import constellation.createTransaction
import org.constellation.Fixtures.id
import org.constellation.consensus.EdgeProcessor.acceptWithResolveAttempt
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class EdgeProcessorTest extends FlatSpec with MockFactory with OneInstancePerTest with Matchers {
  implicit val mockDao = mock[DAO]

  val sendRequest = SendToAddress(id.address, 1L)
  val tx =
    createTransaction(id.address, sendRequest.dst, sendRequest.amountActual, Fixtures.keyPair)
  val mockCpb = mock[CheckpointBlock]
  val cpc = CheckpointCache(Some(mockCpb))
  (mockDao.id _) expects () returns (id)
  (mockCpb.parentSOEBaseHashes()(_: DAO)) expects (*) returns (Seq.empty)

  "handleFinishedCheckpoint" should "Attempt to resolve before accepting checkpoint with parents missing" in {
    assertThrows[RuntimeException] {
      acceptWithResolveAttempt(cpc, 100)(mockDao)
    }
  }
}
