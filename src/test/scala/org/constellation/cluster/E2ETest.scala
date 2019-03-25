package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.softwaremill.sttp.{Response, StatusCodes}
import org.constellation._
import org.constellation.consensus.StoredSnapshot
import org.constellation.primitives.{ChannelProof, _}
import org.constellation.util.{APIClient, Simulation}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class E2ETest extends E2E {
  val updatePasswordReq = UpdatePassword(
    Option(System.getenv("DAG_PASSWORD")).getOrElse("updatedPassword")
  )

  def updatePasswords(apiClients: Seq[APIClient]): Seq[Response[String]] =
    apiClients.map { client =>
      val response = client.postSync("password/update", updatePasswordReq)
      client.setPassword(updatePasswordReq.password)
      response
    }

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

  val totalNumNodes = 3

  private val n1 = createNode(randomizePorts = false)

  //private val address1 = n1.getInetSocketAddress

  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes - 1)(
    i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i * 2) + 2)
  )

  private val apis: Seq[APIClient] = nodes.map { _.getAPIClient() }

  private val addPeerRequests = nodes.map { _.getAddPeerRequest }

  private val initialAPIs = apis

  // val n1App = new ConstellationApp(apis.head)

  // val constellationAppSim = new ConstellationAppSim(sim, n1App)

  "E2E Run" should "demonstrate full flow" in {
    logger.info("API Ports: " + apis.map { _.apiPort })

    assert(Simulation.run(initialAPIs, addPeerRequests))

    val firstAPI = apis.head
    val allChannels = firstAPI.getBlocking[Seq[String]]("channels")


    val channelProof = allChannels.map{ channelId =>
      firstAPI.getBlocking[Option[ChannelProof]]("channel/" + channelId, timeout = 90.seconds)
    }
    assert(channelProof.exists{_.nonEmpty})



    // val deployResponse = constellationAppSim.openChannel(apis)

    val downloadNode = createNode(seedHosts = Seq(HostPort("localhost", 9001)),
                                  randomizePorts = false,
                                  portOffset = 50)

    val downloadAPI = downloadNode.getAPIClient()
    logger.info(s"DownloadNode API Port: ${downloadAPI.apiPort}")
    assert(Simulation.checkReady(Seq(downloadAPI)))
    // deployResponse.foreach{ res => res.foreach(constellationAppSim.postDownload(apis.head, _))}

    // messageSim.postDownload(apis.head)

    // TODO: Change to wait for the download node to participate in several blocks.
    Thread.sleep(20 * 1000)

    val allNodes = nodes :+ downloadNode

    val allAPIs: Seq[APIClient] = allNodes.map { _.getAPIClient() } //apis :+ downloadAPI
    val updatePasswordResponses = updatePasswords(allAPIs)
    assert(updatePasswordResponses.forall(_.code == StatusCodes.Ok))
    assert(Simulation.healthy(allAPIs))
    //  Thread.sleep(1000*1000)

    // Stop transactions
    Simulation.triggerRandom(allAPIs)
    Simulation.triggerCheckpointFormation(allAPIs)

    Simulation.logger.info("Stopping transactions to run parity check")

    Simulation.awaitConditionMet("Accepted checkpoint blocks number differs across the nodes",
                                 allAPIs.map { _.metrics("checkpointAccepted") }.distinct.size == 1,
                                 maxRetries = 6,
                                 delay = 10000)
    Simulation.awaitConditionMet(
      "Accepted transactions number differs across the nodes",
      allAPIs.map { _.metrics("transactionAccepted") }.distinct.size == 1,
      maxRetries = 6,
      delay = 10000
    )

    val storedSnapshots = allAPIs.map { _.simpleDownload() }

    // constellationAppSim.dumpJson(storedSnapshots)

    // TODO: Move to separate test

    // TODO: This is flaky and fails randomly sometimes
    val snaps = storedSnapshots.toSet
      .map { x: Seq[StoredSnapshot] => // May need to temporarily ignore messages for partitioning changes?
        x.map { _.checkpointCache.flatMap { _.checkpointBlock} }.toSet
      }

    // Not inlining this for a reason -- the snaps object is quite large,
    // and scalatest tries to be smart when the assert fails and dumps the object to stdout,
    // overwhelming the test output.
    // By extracting to a var I should get sane output on failure.
    // Obviously figuring out why this randomly fails would be even better, but we're working on that.
    val sizeEqualOnes = snaps.size == 1
    assert(sizeEqualOnes)

  }
}

case class BlockDumpOutput(
  blockSoeHash: String,
  blockParentSOEHashes: Seq[String],
  blockMessageValid: Option[Boolean],
  messageParent: Option[String],
  messageHash: Option[String]
)
