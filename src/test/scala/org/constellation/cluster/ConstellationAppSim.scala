package org.constellation.cluster
import org.constellation.{Channel, ConstellationApp}
import org.constellation.consensus.StoredSnapshot
import org.constellation.primitives.Schema.Id
import org.constellation.primitives.{ChannelMessage, ChannelMessageMetadata, ChannelProof, SensorData}
import org.constellation.util.{APIClient, Simulation}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
import scala.concurrent.duration._

class ConstellationAppSim(constellationApp: ConstellationApp)(
  implicit val executionContext: ExecutionContext
){

  val sim = Simulation

  private val schemaStr = SensorData.jsonSchema
  private var broadcastedMessages: Seq[ChannelMessage] = Seq.empty[ChannelMessage]

  def assertGenesisAccepted(apis: Seq[APIClient])(resp: Channel) = {
    sim.awaitConditionMet(
      "Test channel genesis not stored",
      apis.forall {
        _.getBlocking[Option[ChannelMessageMetadata]](
          "messageService/" + resp.channelId
        ).exists(_.blockHash.nonEmpty)
      }
    )
  }

  def messagesInSnapshots(
                                  channelId: String,
                                  apis: Seq[APIClient],
                                  maxRetries: Int = 30,
                                  delay: Long = 3000
                                ): Boolean = {
    sim.awaitConditionMet(
      s"Messages for ${channelId} not found in snapshot", {
        apis.forall { a =>
          val res = a.getBlocking[Option[ChannelProof]]("channel/" + channelId, timeout = 120.seconds)
          res.nonEmpty
        }
      },
      maxRetries,
      delay
    )
  }

  def messagesReceived(
                        channelId: String,
                        apis: Seq[APIClient],
                        maxRetries: Int = 10,
                        delay: Long = 3000
                      ): Boolean = {
    sim.awaitConditionMet(
      s"Peer health checks failed", {
        apis.forall { a =>
          val res = a.getBlocking[Option[ChannelMessageMetadata]]("messageService/" + channelId, timeout = 30 seconds)
//          res.map(_.channelMessageMetadata.channelMessage.signedMessageData.data)
          res.nonEmpty
        }
      },
      maxRetries,
      delay
    )
  }

  def generateChannelMessages(channel: Channel, numMessages: Int = 1): Seq[SensorData] = {
          val validNameChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray.map { _.toString }.toSeq
          val invalidNameChars = validNameChars.map { _.toLowerCase }
          val messagesToBroadcastMessages: Seq[SensorData] = (0 until 5).flatMap { batchNumber =>
            import constellation._
            val validMessages = Seq.fill(batchNumber % 2) {
              SensorData(
                Random.nextInt(100),
                Seq.fill(numMessages) { Random.shuffle(validNameChars).head }.mkString
//                channel.channelId
              )
            }
            val invalidMessages = Seq.fill((batchNumber + 1) % 2) {
              SensorData(
                Random.nextInt(100) + 500,
                Seq.fill(numMessages) { Random.shuffle(invalidNameChars).head }.mkString
//                channel.channelId
              )
            }
            validMessages ++ invalidMessages
          }
    messagesToBroadcastMessages
  }

  def postDownload(firstAPI: APIClient = constellationApp.clientApi, channel: Channel) = {
    sim.logger.info(s"channel ${channel.channelId}")
    val allChannels = firstAPI.getBlocking[Seq[String]]("channels")
    sim.logger.info(s"message channel ${allChannels}")
    val messageChannels = allChannels.filterNot { _ == channel.channelId }
    val messagesWithinSnapshot = messageChannels.flatMap(msg => firstAPI.getBlocking[Option[ChannelProof]]("channel/" + msg, timeout = 30 seconds))
    sim.logger.info(s"messageWithinSnapshot ${messagesWithinSnapshot}")

    assert(messagesWithinSnapshot.nonEmpty)

    messagesWithinSnapshot.foreach {
      proof =>
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
  def messagesValid(proof: ChannelProof) = {
      val m = proof.channelMessageMetadata
      val hasSnapshotHash = m.snapshotHash.nonEmpty
      val hasBlockHash = m.blockHash.nonEmpty
      val checkpointMessageProofVerified = proof.checkpointMessageProof.verify()
      val checkpointProofVerified = proof.checkpointProof.verify()
      val blockhashContainsCheckpointProof = m.blockHash.contains { proof.checkpointProof.input }
      val hashesSame = m.channelMessage.signedMessageData.signatures.hash == proof.checkpointMessageProof.input
      hasSnapshotHash && hasBlockHash && checkpointMessageProofVerified && checkpointProofVerified && blockhashContainsCheckpointProof && hashesSame
  }

//  def dumpJson(
//                storedSnapshots: Seq[Seq[StoredSnapshot]]
//              ): Unit = {
//
//    var numInvalid = 0
//
//    val messagesInChannelWithBlocks = storedSnapshots.head.flatMap { s =>
//      s.checkpointCache.map { cache =>
//        val block = cache.checkpointBlock.get
//        val relevantMessages = block.checkpoint.edge.data.messages
//          .filter { broadcastedMessages.contains }
//        val messageParent = relevantMessages.map {
//          _.signedMessageData.data.previousMessageHash
//        }.headOption
//        val messageHash = relevantMessages.map { _.signedMessageData.hash }.headOption
//
//        val valid = relevantMessages.map { m =>
//          val isValid = SensorData
//            .validate(
//              m.signedMessageData.data.message
//            )
//            .isSuccess
//          if (!isValid) numInvalid += 1
//          isValid
//        }.headOption
//        BlockDumpOutput(block.soeHash, block.parentSOEHashes, valid, messageParent, messageHash)
//      }
//    }
//
//    // TODO: Duplicate messages appearing sometimes but not others?
//    println(s"Num invalid $numInvalid")
//
//    val ids = messagesInChannelWithBlocks.map { _.blockSoeHash }.zipWithIndex.toMap
//    val msgToBlock = messagesInChannelWithBlocks.flatMap { z =>
//      z.messageHash.map { _ -> z.blockSoeHash }
//    }.toMap
//
//    import constellation._
//    val rendered = messagesInChannelWithBlocks.map {
//      case BlockDumpOutput(hash, parents, isValid, msgParent, msgHash) =>
//        val msgParentId = msgParent
//          .flatMap { parent =>
//            msgToBlock.get(parent).flatMap { ids.get }.map { Seq(_) }
//          }
//          .getOrElse(Seq())
//
//        val id = ids(hash)
//        val parentsId = parents.flatMap { ids.get } ++ msgParentId
//        val color = isValid
//          .map { b =>
//            if (b) "green" else "red"
//          }
//          .getOrElse("blue")
//        Map("id" -> id, "parentIds" -> parentsId, "color" -> color)
//    }.json
//    println(rendered)
//
//  }

}