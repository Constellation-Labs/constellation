package org.constellation.domain.redownload

import cats.effect.{Blocker, ContextShift, IO}
import org.constellation.checkpoint.CheckpointAcceptanceService
import org.constellation.p2p.Cluster
import org.constellation.serialization.KryoSerializer
import org.constellation.util.Metrics
import org.constellation.{DAO, TestHelpers}
import cats.implicits._
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class DownloadServiceTest
    extends AnyFreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfterEach
    with ArgumentMatchersSugar {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  var cluster: Cluster[IO] = _
  var downloadService: DownloadService[IO] = _
  var metrics: Metrics = _
  var redownloadService: RedownloadService[IO] = _
  var checkpointAcceptanceService: CheckpointAcceptanceService[IO] = _
  implicit var dao: DAO = _
  KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()

  override def beforeEach() = {
    dao = TestHelpers.prepareMockedDAO()
    cluster = dao.cluster
    redownloadService = dao.redownloadService
    checkpointAcceptanceService = dao.checkpointAcceptanceService
    metrics = dao.metrics
    downloadService = new DownloadService(
      redownloadService,
      cluster,
      checkpointAcceptanceService,
      dao.apiClient,
      metrics,
      ExecutionContext.global,
      Blocker.liftExecutionContext(ExecutionContext.global)
    )

    dao.blacklistedAddresses.clear shouldReturnF Unit
    dao.transactionChainService.clear shouldReturnF Unit
    dao.addressService.clear shouldReturnF Unit
    dao.soeService.clear shouldReturnF Unit
  }

  "clearDataBeforeDownload" - {
    "clears data" - {
      "from blacklistedAddress service" in {
        val check = downloadService.clearDataBeforeDownload()

        check.unsafeRunSync()

        dao.blacklistedAddresses.clear.was(called)
      }

      "from transactionChainService service" in {
        val check = downloadService.clearDataBeforeDownload()

        check.unsafeRunSync()

        dao.transactionChainService.clear.was(called)
      }

      "from addressService service" in {
        val check = downloadService.clearDataBeforeDownload()

        check.unsafeRunSync()

        dao.addressService.clear.was(called)
      }

      "from soeService service" in {
        val check = downloadService.clearDataBeforeDownload()

        check.unsafeRunSync()

        dao.soeService.clear.was(called)
      }
    }
  }

  "downloadAndAcceptGenesis" - {
    "test to be defined after genesis refactor".ignore {}
  }

}
