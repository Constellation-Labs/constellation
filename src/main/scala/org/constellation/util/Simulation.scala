package org.constellation.util

import java.security.KeyPair
import java.util.concurrent.ForkJoinPool

import cats.effect.{IO, Sync}
import com.softwaremill.sttp.Response
import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.primitives.Schema._
import org.constellation.primitives._
import org.constellation.schema.Id
import org.constellation.{ConstellationNode, PeerMetadata}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Random

object Simulation {

  val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(new ForkJoinPool(100))

  def healthy(apis: Seq[APIClient]): Boolean = {
    val responses = apis.map(a => {
      val res = a.getString("health", timeout = 15.seconds)
      res
    })
    Future.sequence(responses).map(_.forall(_.isSuccess)).get(15)
  }

  def hasGenesis(apis: Seq[APIClient]): Boolean = {
    val responses = apis.map(a => {
      val res = a.getString("hasGenesis", timeout = 100.seconds)
      res
    })
    Future.sequence(responses).map(_.forall(_.isSuccess)).get(120)
  }

  def getCheckpointTips(apis: Seq[APIClient]): Seq[Map[String, CheckpointBlock]] = {
    val responses = apis.map(a => {
      val res =
        a.getNonBlocking[Map[String, CheckpointBlock]]("checkpointTips", timeout = 100.seconds)
      res
    })

    Future.sequence(responses).get(120)
  }

  def setIdLocal(apis: Seq[APIClient]): Unit = {
    val responses = apis.map(a => {
      logger.info(s"Getting id for ${a.hostName}:${a.apiPort}")
      val r = a.getNonBlocking[Id]("id", timeout = 60.seconds)
      r.foreach(id => a.id = id)
      r
    })

    Future.sequence(responses).get()
  }

  def setExternalIP(apis: Seq[APIClient]): Boolean = {
    val responses = apis.map { a =>
      a.post("ip", a.hostName + ":" + a.udpPort, timeout = 30.seconds)
    }

    Future.sequence(responses).map(_.forall(_.isSuccess)).get(60)
  }

  def verifyGenesisReceived(apis: Seq[APIClient]): Boolean = {
    val responses = apis.map { a =>
      a.getNonBlocking[MetricsResult]("metrics")
    }

    Future
      .sequence(responses)
      .getOpt()
      .exists(_.forall(gbmd => gbmd.metrics("numValidBundles").toInt >= 1))
  }

  def genesis(apis: Seq[APIClient], startingAcctBalances: Seq[AccountBalance] = Nil): GenesisObservation = {
    val balances = fakeBalances(apis) ++ startingAcctBalances
    apis.head.postBlocking[GenesisObservation]("genesis/create", balances)
  }

  def balanceForAddress(apis: Seq[APIClient], address: String): Seq[String] = {
    val responses = apis.map(a => a.getString(s"balance/$address", timeout = 5.seconds))
    Future.sequence(responses).get().map(_.body.getOrElse(""))
  }

  def getLastTxRefForAddress(api: APIClient, address: String): LastTransactionRef =
    api.getBlocking[LastTransactionRef]("prevTxRef", timeout = 5.seconds)

  def addPeer(
    api: APIClient,
    peer: PeerMetadata
  )(implicit executionContext: ExecutionContext): Future[Response[String]] =
    api.post("addPeer", peer, 60.seconds)

  def addPeerWithRegistrationFlow(
    api: APIClient,
    peer: HostPort
  )(implicit executionContext: ExecutionContext): Future[Response[String]] =
    api.post("peer/add", peer, 60.seconds)

  def assignReputations(apis: Seq[APIClient]): Unit = {
    val responses =
      apis.map { api =>
        val others = apis.filter {
          _ != api
        }
        val havePublic = Random.nextDouble() > 0.5
        val haveSecret = Random.nextDouble() > 0.5 || havePublic
        api.post("reputation", others.map { o =>
          UpdateReputation(
            o.id,
            if (haveSecret) Some(Random.nextDouble()) else None,
            if (havePublic) Some(Random.nextDouble()) else None
          )
        })

      }

    Future.sequence(responses).get()
  }

  def randomNode(apis: Seq[APIClient]) = apis(Random.nextInt(apis.length))

  def fakeBalances(apis: Seq[APIClient]): Seq[AccountBalance] =
    apis
      .map(api => AccountBalance(api.id.address, 999))

  def randomOtherNode(not: APIClient, apis: Seq[APIClient]): APIClient =
    apis.filter {
      _ != not
    }(Random.nextInt(apis.length - 1))

  def checkGenesis(
    apis: Seq[APIClient],
    maxRetries: Int = 10,
    delay: Long = 3000
  ): Boolean =
    awaitConditionMet(
      s"Genesis not stored", {
        val responses = apis.map { a =>
          val maybeObservation = a.getNonBlocking[Option[GenesisObservation]]("genesis")
          maybeObservation.foreach(
            _.foreach(_ => logger.info(s"Genesis stored on ${a.hostName} ${a.apiPort}"))
          )
          maybeObservation
        }

        Future.sequence(responses).getOpt().exists(_.forall(_.nonEmpty))
      },
      maxRetries,
      delay
    )

  def checkReady(
    apis: Seq[APIClient],
    maxRetries: Int = 20,
    delay: Long = 3000
  ): Boolean =
    awaitMetric(
      "Node state not ready",
      _.get("nodeState").contains("Ready"),
      apis,
      maxRetries,
      delay
    )

  def awaitMetric(
    err: String,
    t: Map[String, String] => Boolean,
    apis: Seq[APIClient],
    maxRetries: Int = 10,
    delay: Long = 3000
  ): Boolean =
    awaitConditionMet(
      err, {
        val responses = apis.map { a =>
          a.metricsAsync
        }
        Future
          .sequence(responses)
          .getOpt()
          .exists(_.forall(t(_)))
      },
      maxRetries,
      delay
    )

  def checkPeersHealthy(
    apis: Seq[APIClient],
    maxRetries: Int = 10,
    delay: Long = 3000
  ): Boolean =
    awaitConditionMet(
      s"Peer health checks failed", {
        val responses = apis.map { a =>
          val q = a.postNonBlockingEmpty[Seq[(Id, Boolean)]]("peerHealthCheck")
          q.failed.foreach { e =>
            logger.warn(s"${a.hostPortForLogging} failed!", e)
          }
          q.foreach { t =>
            logger.info(s"${a.hostPortForLogging} succeeded $t!")
          }
          q
        }
        Future
          .sequence(responses)
          .getOpt()
          .exists(
            s =>
              s.forall { res =>
                res.forall(_._2) && res.size == apis.size - 1
              }
          )
      },
      maxRetries,
      delay
    )

  def checkHealthy(
    apis: Seq[APIClient],
    maxRetries: Int = 30,
    delay: Long = 5000
  ): Boolean =
    awaitConditionMet(
      s"Unhealthy nodes", {
        val futures = apis.map { a =>
          a.getString("health")
            .map(_.isSuccess)
            .recover {
              case e: Exception => logger.warn(s"Failure on: ${a.hostName}:${a.apiPort}", e); false
            }
        }

        Future.sequence(futures).getOpt().exists(_.forall(x => x))
      },
      maxRetries,
      delay
    )

  def checkSnapshot(
    apis: Seq[APIClient],
    num: Int = 2,
    maxRetries: Int = 100,
    delay: Long = 10000
  ): Boolean =
    awaitConditionMet(
      s"Less than $num snapshots", {
        val responses = apis.map { a =>
          a.metricsAsync
        }
        Future
          .sequence(responses)
          .getOpt()
          .exists(_.forall { m =>
            m.get(Metrics.snapshotCount).exists {
              _.toInt >= num
            }
          })

      },
      maxRetries,
      delay
    )

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
  ): Boolean =
    awaitConditionMet(
      s"Accepted checkpoints below $numAccepted", {
        val responses = apis.map { a =>
          a.metricsAsync
        }
        Future
          .sequence(responses)
          .getOpt()
          .exists(_.forall { m =>
            m.get(Metrics.checkpointAccepted).exists {
              _.toInt > numAccepted
            }
          })
      },
      maxRetries,
      delay
    )

  def sendRandomTransaction(apis: Seq[APIClient]): Future[Response[String]] = {
    val src = randomNode(apis)
    val dst = randomOtherNode(src, apis).id.address

    val s = SendToAddress(dst, Random.nextInt(1000).toLong)
    src.post("send", s)
  }

  def enableCheckpointFormation(apis: Seq[APIClient]): Seq[Response[String]] = {
    val responses = apis.map(_.postNonBlockingEmptyString("checkpointFormation"))
    Future.sequence(responses).get()
  }

  def disableCheckpointFormation(apis: Seq[APIClient]): Seq[Response[String]] = {
    val responses = apis.map(_.deleteNonBlockingEmptyString("checkpointFormation"))
    Future.sequence(responses).get()
  }

  def enableRandomTransactions(apis: Seq[APIClient]): Seq[Response[String]] = {
    val responses = apis.map(_.postNonBlockingEmptyString("random"))
    Future.sequence(responses).get()
  }

  def disableRandomTransactions(apis: Seq[APIClient]): Seq[Response[String]] = {
    val responses = apis.map(_.deleteNonBlockingEmptyString("random"))
    Future.sequence(responses).get()
  }

  def restoreState(apis: Seq[APIClient]): Seq[Response[String]] = {
    val responses = apis.map(_.postNonBlockingEmptyString("restore"))
    Future.sequence(responses).get()
  }

  def setReady(apis: Seq[APIClient]): Unit = {
    val responses = apis.map(_.postNonBlockingEmptyString("ready"))
    Future.sequence(responses).get()
  }

  def getPublicAddressFromKeyPair(keyPair: KeyPair): String =
    keyPair.getPublic.toId.address

  def createDoubleSpendTxs(
    node1: ConstellationNode,
    node2: ConstellationNode,
    src: String,
    dst: String,
    keyPair: KeyPair
  ): IO[Seq[Transaction]] =
    for {
      firstTx <- node1.dao.transactionService.createTransaction(src, dst, 1L, keyPair)
      firstDoubleSpendTx <- Sync[IO].pure(Transaction(firstTx.edge, LastTransactionRef.empty, isTest = true))
      secondTx <- node2.dao.transactionService.createTransaction(src, dst, 1L, keyPair)
      secondDoubleSpendTx <- Sync[IO].pure(Transaction(secondTx.edge, LastTransactionRef.empty, isTest = true))

      _ <- node1.dao.transactionService.put(TransactionCacheData(firstDoubleSpendTx))
      _ <- node1.dao.transactionService.put(TransactionCacheData(secondDoubleSpendTx))
    } yield Seq(firstDoubleSpendTx, secondDoubleSpendTx)

  def addPeersFromRequest(apis: Seq[APIClient], addPeerRequests: Seq[PeerMetadata]): Unit = {
    val addPeers = apis.flatMap { a =>
      addPeerRequests.zip(apis).filter(_._2 != a).map {
        case (add, a2) =>
          val addAdjusted =
            if (a.internalPeerHost.nonEmpty && a2.internalPeerHost.nonEmpty)
              add
            else
              add.copy(auxHost = "")
          addPeer(a, addAdjusted)
      }
    }

    val results = Future.sequence(addPeers).get()
    assert(results.forall(_.isSuccess))
  }

  def addPeersFromRegistrationRequest(apis: Seq[APIClient], addPeerRequests: Seq[PeerMetadata]): Unit = {

    val addPeers = apis.flatMap { a =>
      addPeerRequests.zip(apis).filter(_._2 != a).map {
        case (add, a2) =>
          val addAdjusted =
            if (a.internalPeerHost.nonEmpty && a2.internalPeerHost.nonEmpty)
              add
            else
              add.copy(auxHost = "")
          addPeerWithRegistrationFlow(a, HostPort(addAdjusted.host, addAdjusted.httpPort))
      }
    }

    val results = Future.sequence(addPeers).get()
    assert(results.forall(_.isSuccess))

  }

  def run(
    apis: Seq[APIClient],
    addPeerRequests: Seq[PeerMetadata],
    startingBalances: Seq[AccountBalance] = Nil,
    attemptSetExternalIP: Boolean = false,
    useRegistrationFlow: Boolean = false,
    useStartFlowOnly: Boolean = false,
    snapshotCount: Int = 2
  ): Boolean = {

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

    val goe = genesis(apis, startingBalances)
    apis.foreach {
      _.post("genesis/accept", goe)
    }

    assert(checkGenesis(apis))
    logger.info("Genesis validation passed")

    setReady(apis)

    if (!useStartFlowOnly) {

      enableRandomTransactions(apis)
      logger.info("Starting random transactions")

      assert(awaitCheckpointsAccepted(apis, numAccepted = 3))

      disableRandomTransactions(apis)
      logger.info("Stopping random transactions to run parity check")
      disableCheckpointFormation(apis)
      logger.info("Stopping checkpoint formation to run parity check")

      Simulation.awaitConditionMet(
        "Accepted checkpoint blocks number differs across the nodes",
        apis.map { p =>
          val n = Await.result(p.metricsAsync, 4 seconds)(Metrics.checkpointAccepted)
          Simulation.logger.info(s"peer ${p.id} has $n accepted cbs")
          n
        }.distinct.size == 1,
        maxRetries = 3,
        delay = 5000
      )

      logger.info("Checkpoint validation passed")

      enableRandomTransactions(apis)
      logger.info("Starting random transactions")
      enableCheckpointFormation(apis)
      logger.info("Starting checkpoint formation")

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
    } else true

  }

}
