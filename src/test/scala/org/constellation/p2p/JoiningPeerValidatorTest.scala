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

  private val joiningNode: APIClient = mock[APIClient]
  joiningNode.hostName shouldReturn "1.1.1.1:9000"

  private val node: APIClient = mock[APIClient]
  node.hostName shouldReturn "0.0.0.0:9000"

  private val wrongGitCommitHash = "abcd"
  private val goodGitCommitHash = BuildInfo.gitCommit

  before {
    node.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
      Response.ok[String](goodGitCommitHash)
  }

  "validate joining peer" - {
    "should return false if peer is running from different hash commit" in {
      joiningNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](wrongGitCommitHash)

      val result = joiningPeerValidator.isValid(joiningNode).unsafeRunSync()

      result shouldBe false
    }

    "should return true if peer has passed all validation" in {
      joiningNode.getStringF[IO]("buildInfo/gitCommit", *, *)(*)(*) shouldReturnF
        Response.ok[String](goodGitCommitHash)

      val result = joiningPeerValidator.isValid(joiningNode).unsafeRunSync()

      result shouldBe true
    }
  }
}
