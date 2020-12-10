package org.constellation

import cats.effect.{ContextShift, ExitCode, IO, Timer}
import com.decodified.scalassh._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.schema.NodeState.Offline
import org.constellation.schema.{ClusterNode, MetricsResult, NodeState, NodeStateInfo}
import org.constellation.testutils.HttpUtil.{evalRequest, evalRequestForHosts}
import org.constellation.testutils.{CustomMatchers, HttpUtil}
import org.http4s.Uri
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.client.dsl.io._
import org.http4s.dsl.io._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.time.SpanSugar._

import scala.concurrent.ExecutionContext.global
import scala.io.Source

class DownloadSpec extends AnyFreeSpec with CustomMatchers with TestConfig with TerraformOutput with Eventually {

  private val logger = Slf4jLogger.getLogger[IO]

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(
      timeout = scaled(3 minutes),
      interval = scaled(1 second)
    )

  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  private val hostConfigProvider = HostConfigProvider
    .fromHostConfig(
      HostConfig(
        login = PublicKeyLogin(config.ssh.user, config.ssh.keyfile),
        hostKeyVerifier = HostKeyVerifiers.DontVerify
      )
    )

  private val instanceIp: String = terraform.instanceIps.value.last

  "restarting a node and joining cluster" in {
    terraform.instanceIps.value.length should be > 3

    val stopCmd = getResourceAsString("scripts/stop.sh")
    evalSSHCommand(stopCmd)

    eventually {
      getClusterNodesState should contain only (Offline)
    }

    // TODO remove the condition below, once #1453 is fixed
    eventually(Timeout(1 hour)) {
      getClusterNodesState shouldBe empty
    }

    val startCmd = getResourceAsString("scripts/start.sh")
    evalSSHCommand(startCmd)

    eventually {
      NodeState.validForDownload should contain (getNodeState)
    }

    val headIP = s"headIP=${terraform.instanceIps.value.head};"
    val joinCmd = getResourceAsString("scripts/join.sh")
    evalSSHCommand(headIP + joinCmd)

    eventually { NodeState.readyStates should contain (getNodeState) }
  }

  private def getResourceAsString(resourcePath: String) = Source.fromResource(resourcePath).getLines().mkString("\n")

  private def evalSSHCommand(command: String): Unit =
    IO.fromTry(SSH(instanceIp, hostConfigProvider) { _.exec(command) })
      .flatMap { result =>
        val exitCode = result.exitCode.map(ExitCode(_)).getOrElse(ExitCode.Error)
        if (exitCode == ExitCode.Success) {
          logger.info(s"Execution succeeded: stdout = ${result.stdOutAsString()}")
        } else {
          IO.raiseError[Unit](
            new RuntimeException(s"Execution failed: exit code = ${exitCode.code}, stderr = ${result.stdErrAsString()}")
          )
        }
      }
      .unsafeRunSync()

  private def getNodeState: NodeState =
    evalRequest[NodeStateInfo](GET(Uri.unsafeFromString(s"http://$instanceIp:9000/state"))).nodeState

  private def getClusterNodesState: List[NodeState] =
    evalRequestForHosts[List[ClusterNode]](terraform.instanceIps.value.init) {
      _.map(host => GET(Uri.unsafeFromString(s"http://$host:9000/cluster/info")))
    }.flatMap { _.find(_.ip.host == instanceIp).map(_.status) }

}
