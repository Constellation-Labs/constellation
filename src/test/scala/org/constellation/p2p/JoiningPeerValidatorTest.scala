package org.constellation.p2p

import cats.data.Kleisli
import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.client.BuildInfoClientInterpreter
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

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  val apiClient = mock[ClientInterpreter[IO]]
  val joiningPeerValidator: JoiningPeerValidator[IO] = new JoiningPeerValidator[IO](apiClient)

  private val joiningNode = PeerClientMetadata("1.1.1.1", 9000, null)

  private val wrongGitCommitHash = "abcd"
  private val goodGitCommitHash = BuildInfo.gitCommit

  before {
    apiClient.buildInfo shouldReturn mock[BuildInfoClientInterpreter[IO]]
  }

  "validate joining peer" - {
    "should return false if peer is running from different hash commit" in {
      apiClient.buildInfo.getGitCommit() shouldReturn Kleisli.apply[IO, PeerClientMetadata, String] { _ =>
        IO.pure(wrongGitCommitHash)
      }

      val result = joiningPeerValidator.isValid(joiningNode).unsafeRunSync()

      result shouldBe false
    }

    "should return true if peer has passed all validation" in {
      apiClient.buildInfo.getGitCommit() shouldReturn Kleisli.apply[IO, PeerClientMetadata, String] { _ =>
        IO.pure(goodGitCommitHash)
      }

      val result = joiningPeerValidator.isValid(joiningNode).unsafeRunSync()

      result shouldBe true
    }
  }
}
