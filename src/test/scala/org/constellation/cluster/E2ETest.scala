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


  "E2E Run" should "demonstrate full flow" in {
    logger.info("API Ports: " + apis.map { _.apiPort })

    assert(Simulation.run(initialAPIs, addPeerRequests))

    val metadatas = n1.getPeerAPIClient.postBlocking[Seq[ChannelMetadata]]("channel/neighborhood", n1.dao.id)

    println(s"Metadata: $metadatas")

    assert(
      metadatas.nonEmpty,
      "channel neighborhood empty"
    )

    val lightNode = createNode(
      seedHosts = Seq(HostPort("localhost", 9001)),
      portOffset = 20,
      randomizePorts = false,
      isLightNode = true
    )

    val lightNodeAPI = lightNode.getAPIClient()

    Simulation.awaitConditionMet(
      "Light node has no data",
      lightNodeAPI.getBlocking[Seq[String]]("channelKeys").nonEmpty
    )


    val firstAPI = apis.head
    val allChannels = firstAPI.getBlocking[Seq[String]]("channels")

/*
    val channelProof = allChannels.map{ channelId =>
      firstAPI.getBlocking[Option[ChannelProof]]("channel/" + channelId, timeout = 90.seconds)
    }
    assert(channelProof.exists{_.nonEmpty})

*/

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

  /*  // "ConstellationApp"
  ignore should "register a deployed state channel" in {
    sim.triggerRandom(apis)
    val deployResp = n1App.deploy(SensorData.jsonSchema, "channel_1")
    deployResp.map { resp: Option[Channel] =>
      sim.logger.info("deploy response:" + resp.toString)
      assert(resp.exists(r => r.channelId == r.channelOpenRequest.genesisHash))
      assert(resp.exists(_.channelName == "channel_1"))
      assert(resp.forall(r => n1App.channelIdToChannel.get(r.channelId).contains(r)))
    }
  }*/
}

class ConstellationAppSim(constellationApp: ConstellationApp)(
  implicit val executionContext: ExecutionContext
) {
  private val schemaStr = SensorData.jsonSchema
  private val channelName = "test"
  private var broadcastedMessages: Seq[ChannelMessage] = Seq.empty[ChannelMessage]

  def openChannel(apis: Seq[APIClient]): Future[Option[Channel]] = {
    val deployResponse = constellationApp.deploy(schemaStr, channelName)
    deployResponse.foreach { resp =>
      if (resp.isDefined) {
        Simulation.awaitConditionMet(
          "Test channel genesis not stored",
          apis.forall {
            _.getBlocking[Option[ChannelMessageMetadata]](
              "messageService/" + resp.map(_.channelId).getOrElse(channelName)
            ).exists(_.blockHash.nonEmpty)
          }
        )
        resp.foreach { channel: Channel =>
          val validNameChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray.map { _.toString }.toSeq
          val invalidNameChars = validNameChars.map { _.toLowerCase }

          val messagesToBroadcastMessages: Seq[SensorData] = (0 until 10).flatMap { batchNumber =>
            val validMessages = Seq.fill(batchNumber % 2) {
              SensorData(
                Random.nextInt(100),
                Seq.fill(5) { Random.shuffle(validNameChars).head }.mkString
              )
            }
            val invalidMessages = Seq.fill((batchNumber + 1) % 2) {
              SensorData(
                Random.nextInt(100) + 500,
                Seq.fill(5) { Random.shuffle(invalidNameChars).head }.mkString
              )
            }

            val msgs = validMessages ++ invalidMessages
            Simulation.logger.info(
              s"Message batch $batchNumber complete, sent ${msgs.size} messages"
            )
            msgs
          }
        // TODO: Fix type bounds after changing schema
        /*val broadcastResp: Future[Seq[ChannelMessage]] =
          constellationApp.broadcast(messagesToBroadcastMessages)
        broadcastResp.foreach { res: Seq[ChannelMessage] =>
          sim.logger.info(
            s"broadcastResp is: ${res.toString}"
          )
          broadcastedMessages = res
        }*/
        }
      }
    }
    deployResponse
  }

  def postDownload(firstAPI: APIClient = constellationApp.clientApi, channel: Channel) = {
    Simulation.logger.info(s"channel ${channel.channelId}")
    val allChannels = firstAPI.getBlocking[Seq[String]]("channels")
    Simulation.logger.info(s"message channel ${allChannels}")

    val messageChannels = allChannels.filterNot { _ == channel.channelId }
    val messagesWithinSnapshot = messageChannels.flatMap(
      msg => firstAPI.getBlocking[Option[ChannelProof]]("channel/" + msg, timeout = 30 seconds)
    )
    Simulation.logger.info(s"messageWithinSnapshot ${messagesWithinSnapshot}")

    assert(messagesWithinSnapshot.nonEmpty)

    messagesWithinSnapshot.foreach { proof =>
      val m = proof.channelMessageMetadata
      assert(m.snapshotHash.nonEmpty)
      assert(m.blockHash.nonEmpty)
      assert(proof.checkpointMessageProof.verify())
      assert(proof.checkpointProof.verify())
      assert(m.blockHash.contains { proof.checkpointProof.input })
      assert(
        m.channelMessage.signedMessageData.signatures.hash == proof.checkpointMessageProof.input
      )
    }
  }

  def dumpJson(
    storedSnapshots: Seq[Seq[StoredSnapshot]]
  ): Unit = {

    var numInvalid = 0

    val messagesInChannelWithBlocks = storedSnapshots.head.flatMap { s =>
      s.checkpointCache.map { cache =>
        val block = cache.checkpointBlock.get
        val relevantMessages = block.messages
          .filter { broadcastedMessages.contains }
        val messageParent = relevantMessages.map {
          _.signedMessageData.data.previousMessageHash
        }.headOption
        val messageHash = relevantMessages.map { _.signedMessageData.hash }.headOption

        val valid = relevantMessages.map { m =>
          val isValid = SensorData
            .validate(
              m.signedMessageData.data.message
            )
            .isSuccess
          if (!isValid) numInvalid += 1
          isValid
        }.headOption
        BlockDumpOutput(block.soeHash, block.parentSOEHashes, valid, messageParent, messageHash)
      }
    }

    // TODO: Duplicate messages appearing sometimes but not others?
    println(s"Num invalid $numInvalid")

    val ids = messagesInChannelWithBlocks.map { _.blockSoeHash }.zipWithIndex.toMap
    val msgToBlock = messagesInChannelWithBlocks.flatMap { z =>
      z.messageHash.map { _ -> z.blockSoeHash }
    }.toMap

    import constellation._
    val rendered = messagesInChannelWithBlocks.map {
      case BlockDumpOutput(hash, parents, isValid, msgParent, msgHash) =>
        val msgParentId = msgParent
          .flatMap { parent =>
            msgToBlock.get(parent).flatMap { ids.get }.map { Seq(_) }
          }
          .getOrElse(Seq())

        val id = ids(hash)
        val parentsId = parents.flatMap { ids.get } ++ msgParentId
        val color = isValid
          .map { b =>
            if (b) "green" else "red"
          }
          .getOrElse("blue")
        Map("id" -> id, "parentIds" -> parentsId, "color" -> color)
    }.json
    println(rendered)

  }

}

case class BlockDumpOutput(
  blockSoeHash: String,
  blockParentSOEHashes: Seq[String],
  blockMessageValid: Option[Boolean],
  messageParent: Option[String],
  messageHash: Option[String]
)
