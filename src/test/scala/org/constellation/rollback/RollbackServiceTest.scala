package org.constellation.rollback

import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.{ConstellationExecutionContext, DAO}
import org.constellation.storage.SnapshotService
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class RollbackServiceTest
    extends FunSuite
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  var rollbackAccountBalances: RollbackAccountBalances = _
  var rollbackService: RollbackService[IO] = _
  var snapshotService: SnapshotService[IO] = _
  var dao: DAO = _

  before {
    dao = mockDAO
    rollbackAccountBalances = new RollbackAccountBalances
    rollbackService = new RollbackService[IO](
      dao,
      rollbackAccountBalances,
      snapshotService
    )
  }

  test("should pass for data from test resources") {
    val result = rollbackService
      .validate(new RollbackLoader(rollbackDataDirectory = "src/test/resources/rollback_data"))
      .value
      .unsafeRunSync()

    result.isRight shouldBe true
  }

  private def mockDAO: DAO = mock[DAO]
}
