package org.constellation.p2p

import constellation._
import cats.data.Kleisli
import cats.effect.{ContextShift, IO}
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.infrastructure.endpoints.BuildInfoEndpoints.BuildInfoJson
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.infrastructure.p2p.client.BuildInfoClientInterpreter
import org.constellation.serializer.KryoSerializer
import org.constellation.{BuildInfo, ConstellationExecutionContext}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class JoiningPeerValidatorTest
    extends AnyFreeSpec
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

  private val wrongBuildInfo = BuildInfoJson(
    name = "invalid",
    version = "2.2.2",
    sbtVersion = "2.2.2",
    scalaVersion = "2.2.2",
    gitBranch = "invalid",
    gitCommit = "invalid",
    builtAtMillis = 123L,
    builtAtString = "123"
  )
  private val validBuildInfo =
    BuildInfoJson().copy()

  before {
    apiClient.buildInfo shouldReturn mock[BuildInfoClientInterpreter[IO]]
  }

  "validate joining peer" - {
    "should return false if peer is running from different build info hash" in {
      apiClient.buildInfo.getBuildInfo() shouldReturn Kleisli.apply[IO, PeerClientMetadata, BuildInfoJson] { _ =>
        IO.pure(wrongBuildInfo)
      }

      val result = joiningPeerValidator.isValid(joiningNode).unsafeRunSync()

      result shouldBe false
    }

    "should return true if peer has passed all validation" in {
      apiClient.buildInfo.getBuildInfo() shouldReturn Kleisli.apply[IO, PeerClientMetadata, BuildInfoJson] { _ =>
        IO.pure(validBuildInfo)
      }

      val result = joiningPeerValidator.isValid(joiningNode).unsafeRunSync()

      result shouldBe true
    }
  }
}
