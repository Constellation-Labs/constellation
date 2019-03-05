package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import com.softwaremill.sttp.{Response, StatusCodes}
import org.constellation._
import org.constellation.consensus.StoredSnapshot
import org.constellation.util.{APIClient, Simulation}

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


  val totalNumNodes = 3

  private val n1 = createNode(randomizePorts = false)

  //private val address1 = n1.getInetSocketAddress

  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes - 1)(
    i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i * 2) + 2)
  )

  private val apis: Seq[APIClient] = nodes.map { _.getAPIClient() }

  private val addPeerRequests = nodes.map { _.getAddPeerRequest }

  private val sim = new Simulation()

  private val initialAPIs = apis

//   val n1App = new ConstellationApp(apis.head)
//
//   val constellationAppSim = new ConstellationAppSim(sim, n1App)

  "E2E Run" should "demonstrate full flow" in {
    logger.info("API Ports: " + apis.map { _.apiPort })

    assert(sim.run(initialAPIs, addPeerRequests))

    // val deployResponse = constellationAppSim.openChannel(apis)

    val downloadNode = createNode(seedHosts = Seq(HostPort("localhost", 9001)),
                                  randomizePorts = false,
                                  portOffset = 50)

    val downloadAPI = downloadNode.getAPIClient()
    logger.info(s"DownloadNode API Port: ${downloadAPI.apiPort}")
    assert(sim.checkReady(Seq(downloadAPI)))
    // deployResponse.foreach{ res => res.foreach(constellationAppSim.postDownload(apis.head, _))}

    // messageSim.postDownload(apis.head)

    Thread.sleep(20 * 1000)

    val allNodes = nodes :+ downloadNode

    val allAPIs: Seq[APIClient] = allNodes.map { _.getAPIClient() } //apis :+ downloadAPI
    val updatePasswordResponses = updatePasswords(allAPIs)
    assert(updatePasswordResponses.forall(_.code == StatusCodes.Ok))
    assert(sim.healthy(allAPIs))
    // Thread.sleep(1000*1000)

    // Stop transactions
    sim.triggerRandom(allAPIs)

    sim.logger.info("Stopping transactions to run parity check")

    Thread.sleep(30000)

    // TODO: Change assertions to check several times instead of just waiting ^ with sleep
    // Follow pattern in Simulation.await examples
    assert(allAPIs.map { _.metrics("checkpointAccepted") }.distinct.size == 1)
    assert(allAPIs.map { _.metrics("transactionAccepted") }.distinct.size == 1)

    val storedSnapshots = allAPIs.map { _.simpleDownload() }

    // constellationAppSim.dumpJson(storedSnapshots)

    // TODO: Move to separate test

    // TODO: This is flaky and fails randomly sometimes
    val snaps = storedSnapshots.toSet
      .map { x: Seq[StoredSnapshot] =>
        x.map { _.checkpointCache.flatMap { _.checkpointBlock } }.toSet
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
