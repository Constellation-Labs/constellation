package org.constellation.consensus

import constellation.createTransaction
import org.constellation.Fixtures.id
import org.constellation.consensus.EdgeProcessor.acceptWithResolveAttempt
import org.constellation.primitives.CheckpointBlock
import org.constellation.primitives.Schema._
import org.constellation.{DAO, Fixtures}
import org.mockito.IdiomaticMockito
import org.scalatest.{FlatSpec, OneInstancePerTest}

class EdgeProcessorTest extends FlatSpec with IdiomaticMockito with OneInstancePerTest {
  implicit val mockDao = mock[DAO]

  val sendRequest = SendToAddress(id.address, 1L)
  val tx = createTransaction(id.address, sendRequest.dst, sendRequest.amountActual, Fixtures.kp)
  val mockCpb = mock[CheckpointBlock]
  val cpc = CheckpointCache(Some(mockCpb))
  mockDao.id shouldReturn id
  mockCpb.parentSOEBaseHashes() shouldReturn Seq.empty

  "handleFinishedCheckpoint" should "Attempt to resolve before accepting checkpoint with parents missing" in {
    assertThrows[RuntimeException] {
      acceptWithResolveAttempt(cpc, 100)(mockDao)
    }
  }
}
