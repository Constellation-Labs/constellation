package org.constellation.p2p

import cats.effect.{ContextShift, IO}
import com.softwaremill.sttp.Response
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.util.APIClient
import org.constellation.{BuildInfo, ConstellationExecutionContext}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class JoiningPeerValidatorTest
    extends FreeSpec
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val joiningPeerValidator: JoiningPeerValidator[IO] = new JoiningPeerValidator[IO]()

  private val badNode: APIClient = mock[APIClient]
  badNode.hostName shouldReturn "1.1.1.1:9000"
  private val badNodeAddress: String = "DAG77"

  private val goodNode: APIClient = mock[APIClient]
  goodNode.hostName shouldReturn "0.0.0.0:9000"
  private val goodNodeAddress: String = "DAG89"

  private val wrongStakingAmount = "200"
  private val goodStakingAmount = "9999999"

  private val wrongGitCommitHash = "abcd"
  private val goodGitCommitHash = BuildInfo.gitCommit

  before {
    goodNode.getStringF[IO]("selfAddress", *, *)(*)(*) shouldReturnF
      Response.ok[String](goodNodeAddress)
    goodNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
      Response.ok[String](goodGitCommitHash)
    goodNode.getStringF[IO](s"balance/$goodNodeAddress", *, *)(*)(*) shouldReturnF
      Response.ok[String](goodStakingAmount)
  }

  "is valid joining peer" - {
    "should return false if peer is running from different hash commit" in {
      badNode.getStringF[IO]("selfAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](badNodeAddress)
      badNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](wrongGitCommitHash)
      badNode.getStringF[IO](s"balance/$badNodeAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodStakingAmount)

      val result = joiningPeerValidator.isValid(badNode).unsafeRunSync()

      result shouldBe false
    }

    "should return false if peer has insufficient account balance" in {
      badNode.getStringF[IO]("selfAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](badNodeAddress)
      badNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodGitCommitHash)
      badNode.getStringF[IO](s"balance/$badNodeAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](wrongStakingAmount)

      val result = joiningPeerValidator.isValid(badNode).unsafeRunSync()

      result shouldBe false
    }

    "should return false if peer has not passed more than one validation" - {
      badNode.getStringF[IO]("selfAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](badNodeAddress)
      badNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](wrongGitCommitHash)
      badNode.getStringF[IO](s"balance/$badNodeAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](wrongStakingAmount)

      val result = joiningPeerValidator.isValid(badNode).unsafeRunSync()

      result shouldBe false
    }

    "should return false if the node is unavailable" - {
      badNode.getStringF[IO]("selfAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String]("AddressDoesNotExist")
      badNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodGitCommitHash)
      badNode.getStringF[IO](s"balance/$badNodeAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodStakingAmount)

      val result = joiningPeerValidator.isValid(badNode).unsafeRunSync()

      result shouldBe false
    }

    "should return true if peer has passed all validation" - {
      badNode.getStringF[IO]("selfAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](badNodeAddress)
      badNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodGitCommitHash)
      badNode.getStringF[IO](s"balance/$badNodeAddress", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodStakingAmount)

      val result = joiningPeerValidator.isValid(badNode).unsafeRunSync()

      result shouldBe true
    }
  }
}
