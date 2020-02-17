package org.constellation.util
import java.net.SocketException

import cats.effect.IO
import org.constellation.consensus.ConsensusManager
import org.constellation.p2p.{Cluster, DownloadProcess}
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.NodeType
import org.constellation.schema.Id
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, ProcessingConfig}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

class HealthCheckerTest
    extends FunSpecLike
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfterEach
    with ArgumentMatchersSugar {

  val dao: DAO = mock[DAO]
  dao.id shouldReturn Fixtures.id
  dao.processingConfig shouldReturn ProcessingConfig()

  val downloadProcess: DownloadProcess[IO] = mock[DownloadProcess[IO]]
  val consensusManager: ConsensusManager[IO] = mock[ConsensusManager[IO]]
  val concurrentTipService: ConcurrentTipService[IO] = mock[ConcurrentTipService[IO]]
  val cluster: Cluster[IO] = mock[Cluster[IO]]

  val healthChecker =
    new HealthChecker[IO](
      dao,
      concurrentTipService,
      IO.contextShift(ConstellationExecutionContext.bounded)
    )(
      IO.ioConcurrentEffect(IO.contextShift(ConstellationExecutionContext.bounded)),
      IO.contextShift(ConstellationExecutionContext.bounded)
    )

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    reset(concurrentTipService)
  }
}
