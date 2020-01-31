package org.constellation

import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.primitives._
import org.constellation.util.{APIDirective, MerkleTree}
import org.json4s.JValue
import org.json4s.native.Serialization

trait ChannelAPI extends Json4sSupport with StrictLogging {

  implicit val serialization: Serialization.type
  implicit val stringUnmarshaller: FromEntityUnmarshaller[String]
  implicit val context: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val dao: DAO

  val channelsGetEndpoints: Route = {
    path("channels") {
      complete(dao.threadSafeMessageMemPool.activeChannels.keys.toSeq)
    } ~
      pathPrefix("data") {
        path("channels") {
          complete(ChannelUIOutput(dao.threadSafeMessageMemPool.activeChannels.keys.toSeq))
        } ~
          path("channel" / Segment / "info") { channelId =>
            complete(dao.channelService.lookup(channelId).unsafeRunSync().map { cmd =>
              SingleChannelUIOutput(
                cmd.channelOpen,
                cmd.totalNumMessages,
                cmd.last25MessageHashes,
                cmd.genesisMessageMetadata.channelMessage.signedMessageData.signatures.signatures.head.address
              )
            })
          } ~
          path("channel" / Segment / "schema") { channelId =>
            complete(
              dao.channelService
                .lookup(channelId)
                .unsafeRunSync()
                .flatMap { cmd =>
                  cmd.channelOpen.jsonSchema
                }
                .map { schemaStr =>
                  import org.json4s.native.JsonMethods._
                  pretty(render(parse(schemaStr)))
                }
            )
          }
      } ~
      path("channelKeys") {
        APIDirective.handle(dao.channelService.toMap().map(_.keys.toSeq))(complete(_))
      } ~
      path("channel" / "genesis" / Segment) { channelId =>
        APIDirective.handle(dao.channelService.lookup(channelId))(complete(_))
      } ~
      path("channel" / Segment) { channelHash =>
        def makeProof(cmd: ChannelMessageMetadata, storedSnapshot: StoredSnapshot) = {
          val blocksInSnapshot = storedSnapshot.snapshot.checkpointBlocks.toList
          val blockHashForMessage = cmd.blockHash.get

          if (!blocksInSnapshot.contains(blockHashForMessage)) {
            logger.error("Message block hash not in snapshot")
          }

          val blockProof = MerkleTree(blocksInSnapshot).createProof(blockHashForMessage)

          val block = storedSnapshot.checkpointCache.filter {
            _.checkpointBlock.baseHash == blockHashForMessage
          }.head.checkpointBlock

          val messageProofInput = block.transactions.map {
            _.hash
          } ++ block.messages.map {
            _.signedMessageData.signatures.hash
          }

          val messageProof = MerkleTree(messageProofInput.toList)
            .createProof(cmd.channelMessage.signedMessageData.signatures.hash)

          ChannelProof(
            cmd,
            blockProof,
            messageProof
          )
        }

        val proof = for {
          msg <- dao.messageService.memPool.lookup(channelHash)
          channelRes = Snapshot.findLatestMessageWithSnapshotHash(0, msg)

          res <- if (channelRes.isDefined || msg.isEmpty) {
            channelRes.pure[IO]
          } else {
            dao.messageService
              .lookup(msg.get.channelMessage.signedMessageData.hash)
              .map(Snapshot.findLatestMessageWithSnapshotHash(0, _))
          }

          exists <- res.flatMap(_.snapshotHash.map(dao.snapshotService.exists)).sequence.map(_.forall(_ == true))

          proof = if (exists) {
            res.flatMap { cmd =>
              cmd.snapshotHash.flatMap { snapshotHash =>
                tryWithMetric(
                  {
                    dao.snapshotStorage
                      .readSnapshot(snapshotHash)
                      .value
                      .flatMap(IO.fromEither)
                      .unsafeRunSync // TODO: get rid of unsafeRunSync
                  },
                  "readSnapshotForMessage"
                ).toOption.map(makeProof(cmd, _))
              }
            }
          } else None
        } yield proof

        APIDirective.handle(proof)(complete(_))
      }
  }

  val channelPostEndpoints: Route =
    post {
      pathPrefix("channel") {
        path("open") {
          entity(as[ChannelOpen]) { request =>
            val ioa = IO
              .fromFuture(IO(ChannelMessage.createGenesis(request)))
              .handleErrorWith(_ => IO(ChannelOpenResponse("Failed to open channel")))

            APIDirective.handle(ioa)(complete(_))
          }
        } ~
          path("send") {
            entity(as[ChannelSendRequest]) { send =>
              val ioa = IO
                .fromFuture(IO(ChannelMessage.createMessages(send)))
                .map(_.json)
                .handleErrorWith(_ => IO(ChannelSendResponse("Failed to create messages", Seq()).json))
              APIDirective.handle(ioa)(complete(_))
            }
          } ~
          path("send" / "json") {
            entity(as[ChannelSendRequestRawJson]) { send: ChannelSendRequestRawJson =>
              import constellation._
              val amended = ChannelSendRequest(
                send.channelId,
                send.messages.x[Seq[JValue]].map { _.json }
              )

              val ioa = IO
                .fromFuture(IO(ChannelMessage.createMessages(amended)))
                .map(_.json)
                .handleErrorWith(_ => IO(ChannelOpenResponse("Failed to send raw json").json))
              APIDirective.handle(ioa)(complete(_))
            }
          }
      }
    }

}
