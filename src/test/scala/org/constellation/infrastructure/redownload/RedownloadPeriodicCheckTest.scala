package org.constellation.infrastructure.redownload

import cats.effect.IO
import cats.implicits._
import org.constellation.{DAO, TestHelpers}
import org.constellation.p2p.Cluster
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
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

    dao.redownloadService.fetchAndUpdatePeersProposals shouldReturnF Map.empty
    dao.redownloadService.checkForAlignmentWithMajoritySnapshot(*) shouldReturnF Unit
  }

  "triggerRedownloadCheck" - {
    "calls fetch for peers proposals" in {
      val redownloadPeriodicCheck = new RedownloadPeriodicCheck()

      val trigger = redownloadPeriodicCheck.trigger()
      val cancel = redownloadPeriodicCheck.cancel()

      trigger.guarantee(cancel).unsafeRunSync

      dao.redownloadService.fetchAndUpdatePeersProposals.was(called)
    }

    "calls check for alignment with majority snapshot" in {
      val redownloadPeriodicCheck = new RedownloadPeriodicCheck()

      val trigger = redownloadPeriodicCheck.trigger()
      val cancel = redownloadPeriodicCheck.cancel()

      trigger.guarantee(cancel).unsafeRunSync

      dao.redownloadService.checkForAlignmentWithMajoritySnapshot(*).was(called)
    }
  }

}
