package org.constellation.domain.redownload

import cats.effect.{ContextShift, IO}
import org.constellation.{ConstellationExecutionContext, DAO, TestHelpers}
import org.constellation.p2p.Cluster
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfterEach, FreeSpec, Matchers}

class DownloadServiceTest
    extends FreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfterEach
    with ArgumentMatchersSugar {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  var cluster: Cluster[IO] = _
  var downloadService: DownloadService[IO] = _
  var redownloadService: RedownloadService[IO] = _
  implicit var dao: DAO = _

  override def beforeEach() = {
    dao = TestHelpers.prepareMockedDAO()
    cluster = dao.cluster
    redownloadService = dao.redownloadService
    downloadService = new DownloadService(redownloadService, cluster)

    dao.blacklistedAddresses.clear shouldReturnF Unit
    dao.transactionChainService.clear shouldReturnF Unit
    dao.addressService.clear shouldReturnF Unit
    dao.soeService.clear shouldReturnF Unit
  }

  "requestForFaucet" - {
    "to be defined" ignore {}
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
    "test to be defined after genesis refactor" ignore {}
  }

}
