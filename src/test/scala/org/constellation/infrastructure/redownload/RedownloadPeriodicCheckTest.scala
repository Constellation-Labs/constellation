package org.constellation.infrastructure.redownload

import cats.implicits._
import org.constellation.domain.redownload.ReDownloadPlan
import org.constellation.schema.Id
import org.constellation.storage.RecentSnapshot
import org.constellation.{DAO, TestHelpers}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class RedownloadPeriodicCheckTest
    extends FreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfter
    with ArgumentMatchersSugar {

  implicit var dao: DAO = _

  before {
    dao = TestHelpers.prepareMockedDAO()

    dao.redownloadService.fetchAndSetPeerProposals() shouldReturnF Unit
    dao.redownloadService.recalculateMajoritySnapshot() shouldReturnF mock[ReDownloadPlan]
    dao.redownloadService.checkForAlignmentWithMajoritySnapshot() shouldReturnF Some(List())
  }

  "triggerRedownloadCheck" - {
    "calls fetch for peers proposals" in {
      val redownloadPeriodicCheck = new RedownloadPeriodicCheck()

      val trigger = redownloadPeriodicCheck.trigger()
      val cancel = redownloadPeriodicCheck.cancel()

      (trigger >> cancel).unsafeRunSync

      dao.redownloadService.fetchAndSetPeerProposals().was(called)
    }

    "calls check for alignment with majority snapshot" in {
      val redownloadPeriodicCheck = new RedownloadPeriodicCheck()

      val trigger = redownloadPeriodicCheck.trigger()
      val cancel = redownloadPeriodicCheck.cancel()

      (trigger >> cancel).unsafeRunSync

      dao.redownloadService.checkForAlignmentWithMajoritySnapshot().was(called)
    }
  }

}
