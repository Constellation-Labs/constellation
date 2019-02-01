package org.constellation.cluster

import java.util.concurrent.{ForkJoinPool, TimeUnit}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import com.softwaremill.sttp.{Response, StatusCodes}
import com.typesafe.scalalogging.Logger
import org.constellation.consensus.StoredSnapshot
import org.constellation.primitives._
import org.constellation.primitives.ChannelProof
import org.constellation.util.{APIClient, Simulation, TestNode}
import org.constellation.{ConstellationNode, HostPort, UpdatePassword}
import org.scalatest.{AsyncFlatSpecLike, BeforeAndAfterAll, BeforeAndAfterEach, Matchers}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.{Random, Try}

class E2ETest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val logger = Logger("E2ETest")

  val tmpDir = "tmp"

  implicit val system: ActorSystem = ActorSystem("ConstellationTestNode")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  override def beforeAll(): Unit = {
    // Cleanup DBs
    //Try{File(tmpDir).delete()}
    Try{File(tmpDir).createDirectories()}

  }

  override def afterAll() {
    // Cleanup DBs
    TestNode.clearNodes()
    system.terminate()
    Try{File(tmpDir).delete()}
  }

  def createNode(
                  randomizePorts: Boolean = true,
                  seedHosts: Seq[HostPort] = Seq(),
                  portOffset: Int = 0,
                  isGenesisNode: Boolean = false
                ): ConstellationNode = {
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(new ForkJoinPool(100))

    TestNode(
      randomizePorts = randomizePorts,
      portOffset = portOffset,
      seedHosts = seedHosts,
      isGenesisNode = isGenesisNode
    )
  }
  val updatePasswordReq = UpdatePassword(Option(System.getenv("DAG_PASSWORD")).getOrElse("updatedPassword"))

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

  private val nodes = Seq(n1) ++ Seq.tabulate(totalNumNodes-1)(i => createNode(seedHosts = Seq(), randomizePorts = false, portOffset = (i*2)+2)) // seedHosts = Seq(address1)

  private val apis = nodes.map{_.getAPIClient()}

  private val addPeerRequests = nodes.map{_.getAddPeerRequest}

  private val sim = new Simulation()

  private val initialAPIs = apis

  private val schemaStr = SensorData.jsonSchema

  "E2E Run" should "demonstrate full flow" in {

    logger.info("API Ports: " + apis.map{_.apiPort})

    assert(sim.run(initialAPIs, addPeerRequests, snapshotCount = 5))

    val channelId = "test"

    apis.head.postSync("channel/open", ChannelOpenRequest(channelId, jsonSchema = Some(schemaStr)))
    sim.awaitConditionMet(
      "Test channel genesis not stored",
      apis.forall{
        _.getBlocking[Option[ChannelMessageMetadata]]("messageService/" + channelId).exists(_.blockHash.nonEmpty)
      }
    )

    val genesisChannel = apis.head.getBlocking[Option[ChannelMessageMetadata]]("messageService/" + channelId).get.channelMessage

    val validNameChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray.map{_.toString}.toSeq
    val invalidNameChars = validNameChars.map{_.toLowerCase}

    val expectedMessages = (0 until 10).flatMap{ batchNumber =>
      import constellation._

      val validMessages = Seq.fill(batchNumber % 2) {
        SensorData(
          Random.nextInt(100),
          Seq.fill(5){Random.shuffle(validNameChars).head}.mkString
        )
      }
      val invalidMessages = Seq.fill((batchNumber + 1) % 2) {
        SensorData(
          Random.nextInt(100) + 500,
          Seq.fill(5){Random.shuffle(invalidNameChars).head}.mkString
        )
      }
      val serializedMessages = (validMessages ++ invalidMessages).map{_.json}
      val messages = apis.head.postBlocking[Seq[ChannelMessage]](
        "channel/send",
        ChannelSendRequest(channelId, serializedMessages)
      )
      sim.awaitConditionMet(
        s"Message batch $batchNumber not stored",
        apis.forall{
          _.getBlocking[Option[ChannelMessageMetadata]](
            "messageService/" + messages.head.signedMessageData.signatures.hash
          ).exists(_.blockHash.nonEmpty)
        }
      )
      sim.logger.info(s"Message batch $batchNumber complete, sent ${serializedMessages.size} messages")
      messages
    }

    val downloadNode = createNode(seedHosts = Seq(HostPort("localhost", 9001)), randomizePorts = false, portOffset = 50)

    val downloadAPI = downloadNode.getAPIClient()
    logger.info(s"DownloadNode API Port: ${downloadAPI.apiPort}")
    assert(sim.checkReady(Seq(downloadAPI)))

    val messageChannel = initialAPIs.head.getBlocking[Seq[String]]("channels").filterNot{_ == channelId}.head

    val messageWithinSnapshot = initialAPIs.head.getBlocking[Option[ChannelProof]]("channel/" + messageChannel)
    assert(messageWithinSnapshot.nonEmpty)

    def messageValid(): Unit = messageWithinSnapshot.foreach{ proof =>
      val m = proof.channelMessageMetadata
      assert(m.snapshotHash.nonEmpty)
      assert(m.blockHash.nonEmpty)
      assert(proof.checkpointMessageProof.verify())
      assert(proof.checkpointProof.verify())
      assert(m.blockHash.contains{proof.checkpointProof.input})
      assert(m.channelMessage.signedMessageData.signatures.hash == proof.checkpointMessageProof.input)
    }
    messageValid()

    Thread.sleep(20*1000)

    val allNodes = nodes :+ downloadNode

    val allAPIs: Seq[APIClient] = allNodes.map{_.getAPIClient()} //apis :+ downloadAPI
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
    assert(allAPIs.map{_.metrics("checkpointAccepted")}.distinct.size == 1)
    assert(allAPIs.map{_.metrics("transactionAccepted")}.distinct.size == 1)


    val storedSnapshots = allAPIs.map{_.simpleDownload()}

    var numInvalid = 0

    val messagesInChannelWithBlocks = storedSnapshots.head.flatMap{ s =>
      s.checkpointCache.map{ cache =>
        val block = cache.checkpointBlock.get
        val relevantMessages = block.checkpoint.edge.resolvedObservationEdge.data.get.messages
          .filter{expectedMessages.contains}
          //.filter{_.signedMessageData.data.channelId == channelId}.filterNot{_ == genesisChannel}
        val messageParent = relevantMessages.map{_.signedMessageData.data.previousMessageDataHash}.headOption
        val messageHash = relevantMessages.map{_.signedMessageData.hash}.headOption

        val valid = relevantMessages.map{m =>
          val isValid = SensorData.validate(
            m.signedMessageData.data.message
          ).isSuccess
          if (!isValid) numInvalid += 1
          isValid
        }.headOption
        BlockDumpOutput(block.soeHash, block.parentSOEHashes, valid, messageParent, messageHash)
      }
    }

    // TODO: Duplicate messages appearing sometimes but not others?
    println(s"Num invalid $numInvalid")

    val ids = messagesInChannelWithBlocks.map{_.blockSoeHash}.zipWithIndex.toMap
    val msgToBlock = messagesInChannelWithBlocks.flatMap{z => z.messageHash.map{_ -> z.blockSoeHash}}.toMap

    import constellation._
    val rendered = messagesInChannelWithBlocks.map{
      case BlockDumpOutput(hash, parents, isValid, msgParent, msgHash) =>

        val msgParentId = msgParent.flatMap{
          parent =>
            msgToBlock.get(parent).flatMap{ids.get}.map{Seq(_)}
        }.getOrElse(Seq())

        val id = ids(hash)
        val parentsId = parents.flatMap{ids.get} ++ msgParentId
        val color = isValid.map{ b =>  if (b) "green" else "red"}.getOrElse("blue")
        Map("id" -> id, "parentIds" -> parentsId, "color" -> color)
    }.json
    println(rendered)

    // TODO: This is flaky and fails randomly sometimes
    val snaps = storedSnapshots.toSet
      .map{x : Seq[StoredSnapshot] =>
        x.map{_.checkpointCache.flatMap{_.checkpointBlock}}.toSet
      }

    assert(
      snaps.size == 1
    )

  }


}

case class BlockDumpOutput(
                          blockSoeHash: String,
                          blockParentSOEHashes: Seq[String],
                          blockMessageValid: Option[Boolean],
                          messageParent: Option[String],
                          messageHash: Option[String]
                          )