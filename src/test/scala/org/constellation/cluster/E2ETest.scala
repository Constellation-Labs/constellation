package org.constellation.cluster

import java.util.concurrent.TimeUnit

import akka.util.Timeout
import better.files.File
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp.{Response, StatusCodes}
import org.constellation._
import org.constellation.consensus.StoredSnapshot
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.primitives._
import org.constellation.serializer.KryoSerializer
import org.constellation.storage.RecentSnapshot
import org.constellation.util.{APIClient, Metrics, Simulation}
import scala.concurrent.duration._

import scala.concurrent.Await

class E2ETest extends E2E {

  implicit val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)
  val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  private val updatePasswordReq = UpdatePassword(
    Option(System.getenv("DAG_PASSWORD")).getOrElse("updatedPassword")
  )
  private val totalNumNodes = 4

  private val n1 = createNode(randomizePorts = false)
  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes - 1)(
    i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i * 2) + 2)
  )

  private val apis: Seq[APIClient] = nodes.map(_.getAPIClient())
  private val initialAPIs = apis
  private val addPeerRequests = nodes.map(_.getAddPeerRequest)
  private val storeData = false

  private def updatePasswords(apiClients: Seq[APIClient]): Seq[Response[String]] =
    apiClients.map { client =>
      val response = client.postSync("password/update", updatePasswordReq)
      client.setPassword(updatePasswordReq.password)
      response
    }

  "E2E Run" should "demonstrate full flow" in {
    logger.info("API Ports: " + apis.map(_.apiPort))

    assert(Simulation.run(initialAPIs, addPeerRequests))

    val metadatas =
      n1.getPeerAPIClient.postBlocking[Seq[ChannelMetadata]]("channel/neighborhood", n1.dao.id)

    logger.info(s"Metadata: $metadatas")

    assert(
      metadatas.nonEmpty,
      "channel neighborhood empty"
    )

//    val lightNode = createNode(
//      seedHosts = Seq(HostPort("localhost", 9001)),
//      portOffset = 20,
//      randomizePorts = false,
//      isLightNode = true
//    )
//
//    val lightNodeAPI = lightNode.getAPIClient()
//
//    Simulation.awaitConditionMet(
//      "Light node has no data",
//      lightNodeAPI.getBlocking[Seq[String]]("channelKeys").nonEmpty
//    )

    val firstAPI = apis.head
    val allChannels = firstAPI.getBlocking[Seq[String]]("channels")

    /*
    val channelProof = allChannels.map{ channelId =>
      firstAPI.getBlocking[Option[ChannelProof]]("channel/" + channelId, timeout = 90.seconds)
    }
    assert(channelProof.exists{_.nonEmpty})

     */

    // val deployResponse = constellationAppSim.openChannel(apis)

//    val downloadNode = createNode(
//      seedHosts = Seq(HostPort("localhost", 9001)),
//      randomizePorts = false,
//      portOffset = 50
//    )

//    val downloadAPI = downloadNode.getAPIClient()
//    logger.info(s"DownloadNode API Port: ${downloadAPI.apiPort}")
//    assert(Simulation.checkReady(Seq(downloadAPI)))
    // deployResponse.foreach{ res => res.foreach(constellationAppSim.postDownload(apis.head, _))}

    // messageSim.postDownload(apis.head)

    // TODO: Change to wait for the download node to participate in several blocks.
//    Thread.sleep(20 * 1000)

//    val allNodes = nodes :+ downloadNode
    val allNodes = nodes

    val allAPIs: Seq[APIClient] = allNodes.map {
      _.getAPIClient()
    } //apis :+ downloadAPI
    val updatePasswordResponses = updatePasswords(allAPIs)
    assert(updatePasswordResponses.forall(_.code == StatusCodes.Ok))
    assert(Simulation.healthy(allAPIs))
    //  Thread.sleep(1000*1000)

    Simulation.disableRandomTransactions(allAPIs)
    Simulation.logger.info("Stopping transactions to run parity check")
    Simulation.disableCheckpointFormation(allAPIs)
    Simulation.logger.info("Stopping checkpoint formation to run parity check")

//    TODO: It can fail when redownload occurs fix when  issue #527 is finished
    Simulation.awaitConditionMet(
      "Accepted checkpoint blocks number differs across the nodes",
      allAPIs.map { p =>
        val n = Await.result(p.metricsAsync, 5 seconds)(Metrics.checkpointAccepted)
        Simulation.logger.info(s"peer ${p.id} has $n accepted cbs")
        n
      }.distinct.size == 1,
      maxRetries = 10,
      delay = 10000
    )

    Simulation.awaitConditionMet(
      "Accepted transactions number differs across the nodes",
      allAPIs.map { a =>
        Await.result(a.metricsAsync, 5 seconds).get("transactionAccepted").toList
      }.distinct.size == 1,
      maxRetries = 6,
      delay = 10000
    )

    Simulation.awaitConditionMet(
      "Snapshot hashes differs across cluster",
      allAPIs.toList
        .traverse(
          a => a.getNonBlockingIO[List[RecentSnapshot]]("snapshot/recent")(contextShift)
        )
        .unsafeRunSync()
        .distinct
        .size == 1,
      maxRetries = 30,
      delay = 5000
    )

    val storedSnapshots = allAPIs.map {
      _.simpleDownload()
    }

    storedSnapshots.foreach { s =>
      println("stored snaps =========")
      s.foreach(ss => println(ss.snapshot.hash))
    }

    // constellationAppSim.dumpJson(storedSnapshots)
    if (storeData) saveState(allAPIs)

    // TODO: Move to separate test

    // TODO: This is flaky and fails randomly sometimes
    val snaps = storedSnapshots.toSet.map { x: Seq[StoredSnapshot] => // May need to temporarily ignore messages for partitioning changes?
      x.map {
        _.checkpointCache.flatMap {
          _.checkpointBlock.map(_.baseHash) // TODO: wkoszycki explain the reason behind CheckpointblockCache data distinct doesn't work
        }
      }.toSet
    }

    // Not inlining this for a reason -- the snaps object is quite large,
    // and scalatest tries to be smart when the assert fails and dumps the object to stdout,
    // overwhelming the test output.
    // By extracting to a var I should get sane output on failure.
    // Obviously figuring out why this randomly fails would be even better, but we're working on that.
    val sizeEqualOnes = snaps.size == 1
    assert(sizeEqualOnes)
  }

  private def saveState(allAPIs: Seq[APIClient]): Unit = {
    File("rollback_data/snapshots").createDirectoryIfNotExists().clear()
    storeSnapshotInfo(allAPIs)
    storeSnapshots(allAPIs)
    storeGenesis(allAPIs)
  }

  private def storeSnapshots(allAPIs: Seq[APIClient]) {
    val snapshots: Seq[StoredSnapshot] = allAPIs.flatMap(_.simpleDownload())

    snapshots.foreach(s => {
      better.files
        .File("rollback_data/snapshots", s.snapshot.hash)
        .writeByteArray(KryoSerializer.serializeAnyRef(s))
    })
  }

  private def storeSnapshotInfo(allAPIs: Seq[APIClient]): Unit = {
    val snapshotInfo = allAPIs.map(_.snapshotsInfoDownload())

    snapshotInfo.foreach(s => {
      better.files
        .File("rollback_data", "rollback_info")
        .writeByteArray(KryoSerializer.serializeAnyRef(s))
    })
  }

  private def storeGenesis(allAPIs: Seq[APIClient]): Unit = {
    val genesis =
      allAPIs.flatMap(_.getNonBlockingIO[Option[GenesisObservation]]("genesis")(contextShift).unsafeRunSync())

    genesis.foreach(s => {
      better.files
        .File("rollback_data", "rollback_genesis")
        .writeByteArray(KryoSerializer.serializeAnyRef(s))
    })
  }
}

case class BlockDumpOutput(
  blockSoeHash: String,
  blockParentSOEHashes: Seq[String],
  blockMessageValid: Option[Boolean],
  messageParent: Option[String],
  messageHash: Option[String]
)
