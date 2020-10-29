package org.constellation.infrastructure.redownload

import cats.effect.IO
import cats.syntax.all._
import org.constellation.invertedmap.InvertedMap
import org.constellation.{DAO, TestHelpers}
import org.constellation.p2p.Cluster
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class RedownloadPeriodicCheckTest
    extends AnyFreeSpec
    with Matchers
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with BeforeAndAfter
    with ArgumentMatchersSugar {

  implicit var dao: DAO = _

  before {
    dao = TestHelpers.prepareMockedDAO()

    dao.redownloadService.fetchAndUpdatePeersProposals shouldReturnF InvertedMap.empty
    dao.redownloadService.checkForAlignmentWithMajoritySnapshot(*) shouldReturnF Unit
  }

  "triggerRedownloadCheck" - {
    "calls fetch for peers proposals" in {
      val redownloadPeriodicCheck = new RedownloadPeriodicCheck(30, ExecutionContext.global)

      val trigger = redownloadPeriodicCheck.trigger()
      val cancel = redownloadPeriodicCheck.cancel()

      trigger.guarantee(cancel).unsafeRunSync

      dao.redownloadService.fetchAndUpdatePeersProposals.was(called)
    }

    "calls check for alignment with majority snapshot" in {
      val redownloadPeriodicCheck = new RedownloadPeriodicCheck(30, ExecutionContext.global)

      val trigger = redownloadPeriodicCheck.trigger()
      val cancel = redownloadPeriodicCheck.cancel()

      trigger.guarantee(cancel).unsafeRunSync

      dao.redownloadService.checkForAlignmentWithMajoritySnapshot(*).was(called)
    }
  }

}
