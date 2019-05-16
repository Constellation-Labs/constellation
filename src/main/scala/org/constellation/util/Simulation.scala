package org.constellation.util

import java.util.concurrent.ForkJoinPool

import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.PeerMetadata
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Random, Try}

object Simulation {

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(new ForkJoinPool(100))

  def healthy(apis: Seq[APIClient]): Boolean = {
    apis.forall(a => {
      val res = a.getSync("health", timeout = 100.seconds).isSuccess
      res
    })
  }

  def hasGenesis(apis: Seq[APIClient]): Boolean = {
    apis.forall(a => {
      val res = a.getSync(s"hasGenesis", timeout = 100.seconds).isSuccess
      res
    })
  }

  def getCheckpointTips(apis: Seq[APIClient]): Seq[Map[String, CheckpointBlock]] = {
    apis.map(a => {
      a.getBlocking[Map[String, CheckpointBlock]](s"checkpointTips", timeout = 100.seconds)
    })
  }

  def setIdLocal(apis: Seq[APIClient]): Unit = apis.foreach { a =>
    logger.info(s"Getting id for ${a.hostName}:${a.apiPort}")
    val id = a.getBlocking[Id]("id", timeout = 60.seconds)
    a.id = id
  }

  def setExternalIP(apis: Seq[APIClient]): Boolean =
    apis.forall { a =>
      a.postSync("ip", a.hostName + ":" + a.udpPort).isSuccess
    }

  def verifyGenesisReceived(apis: Seq[APIClient]): Boolean = {
    apis.forall { a =>
      val gbmd = a.getBlocking[MetricsResult]("metrics")
      gbmd.metrics("numValidBundles").toInt >= 1
    }
  }

  def genesis(apis: Seq[APIClient]): GenesisObservation = {
    val ids = apis.map { _.id }
    apis.head.postBlocking[GenesisObservation]("genesis/create", ids.tail.toSet)
  }

  def addPeer(
    apis: Seq[APIClient],
    peer: PeerMetadata
  )(implicit executionContext: ExecutionContext): Seq[Response[String]] = {
    apis.map {
      _.postSync("addPeer", peer)
    }
  }

  def addPeerWithRegistrationFlow(
    apis: Seq[APIClient],
    peer: HostPort
  )(implicit executionContext: ExecutionContext): Seq[Response[String]] = {
    apis.map {
      _.postSync("peer/add", peer)
    }
  }

  def assignReputations(apis: Seq[APIClient]): Unit = apis.foreach { api =>
    val others = apis.filter { _ != api }
    val havePublic = Random.nextDouble() > 0.5
    val haveSecret = Random.nextDouble() > 0.5 || havePublic
    api.postSync("reputation", others.map { o =>
      UpdateReputation(
        o.id,
        if (haveSecret) Some(Random.nextDouble()) else None,
        if (havePublic) Some(Random.nextDouble()) else None
      )
    })
  }

  def randomNode(apis: Seq[APIClient]) = apis(Random.nextInt(apis.length))

  def randomOtherNode(not: APIClient, apis: Seq[APIClient]): APIClient =
    apis.filter { _ != not }(Random.nextInt(apis.length - 1))

  def checkGenesis(
    apis: Seq[APIClient],
    maxRetries: Int = 10,
    delay: Long = 3000
  ): Boolean = {
    awaitConditionMet(
      s"Genesis not stored", {
        apis.forall { a =>
          val maybeObservation = a.getBlocking[Option[GenesisObservation]]("genesis")
          if (maybeObservation.isDefined) {
            logger.info(s"Genesis stored on ${a.hostName} ${a.apiPort}")
          }
          maybeObservation.nonEmpty
        }
      },
      maxRetries,
      delay
    )
  }

  def checkReady(
    apis: Seq[APIClient],
    maxRetries: Int = 20,
    delay: Long = 3000
  ): Boolean = {
    awaitMetric(
      "Node state not ready",
      _.get("nodeState").contains("Ready"),
      apis,
      maxRetries,
      delay
    )
  }

  def awaitMetric(
    err: String,
    t: Map[String, String] => Boolean,
    apis: Seq[APIClient],
    maxRetries: Int = 10,
    delay: Long = 3000
  ): Boolean = {
    awaitConditionMet(
      err, {
        apis.forall { a =>
          t(a.metrics)
        }
      },
      maxRetries,
      delay
    )

  }

  def checkPeersHealthy(
    apis: Seq[APIClient],
    maxRetries: Int = 10,
    delay: Long = 3000
  ): Boolean = {
    awaitConditionMet(
      s"Peer health checks failed", {
        apis.forall { a =>
          val res = a.postBlockingEmpty[Seq[(Id, Boolean)]]("peerHealthCheck")
          res.forall(_._2) && res.size == apis.size - 1
        }
      },
      maxRetries,
      delay
    )
  }

  def checkHealthy(
    apis: Seq[APIClient],
    maxRetries: Int = 30,
    delay: Long = 5000
  ): Boolean = {
    awaitConditionMet(
      s"Unhealthy nodes", {
        apis.forall { a =>
          val attempt = Try { a.getSync("health").isSuccess }
          if (attempt.isFailure) {
            logger.warn(s"Failure on: ${a.hostName}:${a.apiPort}")
          }
          attempt.getOrElse(false)
        }
      },
      maxRetries,
      delay
    )
  }

  def checkSnapshot(
    apis: Seq[APIClient],
    num: Int = 2,
    maxRetries: Int = 100,
    delay: Long = 10000
  ): Boolean = {
    awaitConditionMet(
      s"Less than $num snapshots", {
        apis.forall { a =>
          val m = Try { a.metrics }.toOption.getOrElse(Map())
          m.get(Metrics.snapshotCount).exists { _.toInt >= num }
        }
      },
      maxRetries,
      delay
    )
  }

  def awaitConditionMet(
    err: String,
    t: => Boolean,
    maxRetries: Int = 10,
    delay: Long = 2000
  ): Boolean = {

    var retries = 0
    var done = false

    do {
      retries += 1
      done = t
      logger.info(s"$err Waiting ${delay / 1000} sec. Num attempts: $retries out of $maxRetries")
      Thread.sleep(delay)
    } while (!done && retries < maxRetries)
    assert(done, s"$err TIME EXCEEDED")
    done
  }

  def awaitCheckpointsAccepted(
    apis: Seq[APIClient],
    numAccepted: Int = 5,
    maxRetries: Int = 30,
    delay: Long = 5000
  ): Boolean = {
    awaitConditionMet(
      s"Accepted checkpoints below $numAccepted", {
        apis.forall { a =>
          val maybeString = a.metrics.get(Metrics.checkpointAccepted)
          maybeString.exists(_.toInt > numAccepted)
        }
      },
      maxRetries,
      delay
    )
  }

  def sendRandomTransaction(apis: Seq[APIClient]): Future[Response[String]] = {
    val src = randomNode(apis)
    val dst = randomOtherNode(src, apis).id.address

    val s = SendToAddress(dst, Random.nextInt(1000).toLong)
    src.post("send", s)
  }

  def triggerRandom(apis: Seq[APIClient]): Seq[Response[String]] = {
    apis.map(_.postEmpty("random"))
  }

  def triggerCheckpointFormation(apis: Seq[APIClient]): Seq[Response[String]] = {
    apis.map(_.postEmpty("checkpointFormation"))
  }

  def setReady(apis: Seq[APIClient]): Unit = {
    apis.foreach(_.postEmpty("ready"))
  }

  def addPeersFromRequest(apis: Seq[APIClient], addPeerRequests: Seq[PeerMetadata]): Unit = {
    apis.foreach { a =>
      addPeerRequests.zip(apis).foreach {
        case (add, a2) =>
          if (a2 != a) {
            val addAdjusted =
              if (a.internalPeerHost.nonEmpty && a2.internalPeerHost.nonEmpty) add
              else add.copy(auxHost = "")

            assert(addPeer(Seq(a), addAdjusted).forall(_.isSuccess))
          }
      }
    }
  }

  def addPeersFromRegistrationRequest(apis: Seq[APIClient],
                                      addPeerRequests: Seq[PeerMetadata]): Unit = {
    apis.foreach { a =>
      addPeerRequests.zip(apis).foreach {
        case (add, a2) =>
          if (a2 != a) {
            val addAdjusted =
              if (a.internalPeerHost.nonEmpty && a2.internalPeerHost.nonEmpty) add
              else add.copy(auxHost = "")
            assert(
              addPeerWithRegistrationFlow(Seq(a), HostPort(addAdjusted.host, addAdjusted.httpPort))
                .forall(_.isSuccess)
            )
          }
      }
    }
  }

  def run(
    apis: Seq[APIClient],
    addPeerRequests: Seq[PeerMetadata],
    attemptSetExternalIP: Boolean = false,
    useRegistrationFlow: Boolean = false,
    snapshotCount: Int = 2
  )(implicit executionContext: ExecutionContext): Boolean = {

    assert(checkHealthy(apis))
    logger.info("Health validation passed")

    setIdLocal(apis)

    if (attemptSetExternalIP) {
      assert(setExternalIP(apis))
    }

    logger.info("Adding peers manually")

    if (useRegistrationFlow) {
      addPeersFromRegistrationRequest(apis, addPeerRequests)
    } else addPeersFromRequest(apis, addPeerRequests)

    logger.info("Peers added")
    logger.info("Validating peer health checks")

    assert(checkPeersHealthy(apis))
    logger.info("Peer validation passed")

    val goe = genesis(apis)
    apis.foreach { _.post("genesis/accept", goe) }

    assert(checkGenesis(apis))
    logger.info("Genesis validation passed")

    triggerRandom(apis)

    setReady(apis)

    assert(awaitCheckpointsAccepted(apis, numAccepted = 3))

    logger.info("Checkpoint validation passed")

    var debugChannelName = "debug"
    var attempt = 0

    var channelId: String = ""

    // TODO: Remove after fixing dropped messages
    Simulation.awaitConditionMet(
      "Unable to open channel", {

        debugChannelName = "debug" + attempt

        val channelOpenResponse = apis.head.postBlocking[ChannelOpenResponse](
          "channel/open",
          ChannelOpen(debugChannelName, jsonSchema = Some(SensorData.jsonSchema)),
          timeout = 90.seconds
        )
        attempt += 1
        logger.info(s"Channel open response: ${channelOpenResponse.errorMessage}")
        if (channelOpenResponse.errorMessage == "Success") {
          channelId = channelOpenResponse.genesisHash
          true
        } else false
      }
    )

    logger.info(s"Channel opened with hash $channelId")

    // TODO: Remove after fixing dropped messages
    Simulation.awaitConditionMet(
      "Unable to send message", {

        val csr =
          apis.head.postBlocking[ChannelSendResponse](
            "channel/send",
            ChannelSendRequest(channelId, Seq.fill(2) {
              SensorData.generateRandomValidMessage().json
            })
          )
        Simulation.awaitConditionMet(
          "Unable to find sent message", {

            val cmds = csr.messageHashes.map { h =>
              apis.head.getBlocking[Option[ChannelMessageMetadata]]("messageService/" + h)
            }

            val done = cmds.forall(_.nonEmpty)
            if (done) {
              cmds.flatten.foreach { cmd =>
                val prev = cmd.channelMessage.signedMessageData.data.previousMessageHash
                logger.info(
                  s"msg hash: ${cmd.channelMessage.signedMessageData.hash} previous: $prev"
                )
              }
            }

            done
          }
        )
      }
    )

    assert(awaitCheckpointsAccepted(apis))

    assert(checkSnapshot(apis, num = snapshotCount))

    // TODO: Fix problem with snapshots before enabling this, causes flakiness
    /*
    val channelProof = apis.head.getBlocking[Option[ChannelProof]]("channel/" + channelId, timeout = 90.seconds)
    assert(channelProof.nonEmpty)
     */
    logger.info("Snapshot validation passed")

    true

  }

}
