package org.constellation.rollback

import cats.effect.{ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.mockito.ArgumentMatchersSugar
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

class RollbackServiceTest extends FunSuite with BeforeAndAfter with Matchers with ArgumentMatchersSugar {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  var rollbackAccountBalances: RollbackAccountBalances[IO] = _
  var rollbackLoader: RollbackLoader[IO] = _
  var rollbackService: RollbackService[IO] = _

  before {
    rollbackAccountBalances = new RollbackAccountBalances[IO]
    rollbackLoader = new RollbackLoader[IO](rollbackDataDirectory = "src/test/resources/rollback_data")
    rollbackService = new RollbackService[IO](rollbackAccountBalances, rollbackLoader)
  }

  test("should pass for data from test resources") {
    val result = rollbackService.rollback().value.unsafeRunSync()

    result.isRight shouldBe true
  }
}
