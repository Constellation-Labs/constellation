package org.constellation.util

import java.util.concurrent.ForkJoinPool

import com.typesafe.scalalogging.Logger
import constellation._
import org.constellation.AddPeerRequest
import org.constellation.consensus.SnapshotInfo
import org.constellation.primitives.Schema._
import scalaj.http.HttpResponse

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Random, Try}

class Simulation {

  val logger = Logger(s"Simulation")

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(new ForkJoinPool(100))

  def healthy(apis: Seq[APIClient]): Boolean = {
    apis.forall(a => {
      val res = a.getSync("health", timeoutSeconds = 100).isSuccess
      res
    })
  }

  def hasGenesis(apis: Seq[APIClient]): Boolean = {
    apis.forall(a => {
      val res = a.getSync(s"hasGenesis" , timeoutSeconds = 100).isSuccess
      res
    })
  }

  def getCheckpointTips(apis: Seq[APIClient]): Seq[Map[String, CheckpointBlock]] = {
    apis.map(a => {
      a.getBlocking[Map[String, CheckpointBlock]](s"checkpointTips" , timeoutSeconds = 100)
    })
  }

  def setIdLocal(apis: Seq[APIClient]): Unit = apis.foreach{ a =>
    val id = a.getBlocking[Id]("id")
    a.id = id
  }

  def setExternalIP(apis: Seq[APIClient]): Boolean =
    apis.forall{a => a.postSync("ip", a.hostName + ":" + a.udpPort).isSuccess}

  def verifyGenesisReceived(apis: Seq[APIClient]): Boolean = {
    apis.forall { a =>
      val gbmd = a.getBlocking[MetricsResult]("metrics")
      gbmd.metrics("numValidBundles").toInt >= 1
    }
  }

  def genesis(apis: Seq[APIClient]): GenesisObservation = {
    val ids = apis.map{_.id}
    apis.head.postBlocking[GenesisObservation]("genesis/create", ids.tail.toSet)
  }

  def addPeer(apis: Seq[APIClient], peer: AddPeerRequest)(implicit executionContext: ExecutionContext) = {
    apis.map{
      _.postSync("addPeer", peer)
    }
  }

  def addPeers(apis: Seq[APIClient], peerAPIs: Seq[APIClient])(implicit executionContext: ExecutionContext) = {

    val joinedAPIs = apis.zip(peerAPIs)
    val results = joinedAPIs.flatMap { case (a, peerAPI) =>
      val ip = a.hostName
      logger.info(s"Trying to add nodes to $ip")
      val others = joinedAPIs.filter { case (b, _) => b.id != a.id}
        .map { case (z, bPeer) => AddPeerRequest(z.hostName, z.udpPort, bPeer.apiPort, z.id)}
      others.map {
        n =>
          Future {
            val res = a.postSync("addPeer", n)
            logger.info(s"Tried to add peer $n to $ip res: $res")
          }
      }
    }
    results
  }

  def verifyPeersAdded(apis: Seq[APIClient]): Boolean = apis.forall { api =>
    val peers = api.getBlocking[Seq[Peer]]("peerids")
    logger.info("Peers length: " + peers.length)
    peers.length == (apis.length - 1)
  }

  def assignReputations(apis: Seq[APIClient]): Unit = apis.foreach{ api =>
    val others = apis.filter{_ != api}
    val havePublic = Random.nextDouble() > 0.5
    val haveSecret = Random.nextDouble() > 0.5 || havePublic
    api.postSync("reputation", others.map{o =>
      UpdateReputation(
        o.id,
        if (haveSecret) Some(Random.nextDouble()) else None,
        if (havePublic) Some(Random.nextDouble()) else None
      )
    })
  }

  def randomNode(apis: Seq[APIClient]) = apis(Random.nextInt(apis.length))

  def randomOtherNode(not: APIClient, apis: Seq[APIClient]): APIClient = apis.filter{_ != not}(Random.nextInt(apis.length - 1))

  def checkGenesis(
                                apis: Seq[APIClient],
                                maxRetries: Int = 10,
                                delay: Long = 3000
                              ): Boolean = {
    awaitConditionMet(
      s"Genesis not stored",
      {
        apis.forall{ a =>
          a.getBlocking[Option[GenesisObservation]]("genesis").nonEmpty
        }
      }, maxRetries, delay
    )
  }

  def checkPeersHealthy(
                                apis: Seq[APIClient],
                                maxRetries: Int = 10,
                                delay: Long = 3000
                              ): Boolean = {
    awaitConditionMet(
      s"Peer health checks failed",
      {
        apis.forall{ a =>
          val res = a.postBlockingEmpty[Seq[(Id, Boolean)]]("peerHealthCheck")
          res.forall(_._2) && res.size == apis.size - 1
        }
      }, maxRetries, delay
    )
  }


  def checkHealthy(
                                apis: Seq[APIClient],
                                maxRetries: Int = 10,
                                delay: Long = 3000
                              ): Boolean = {
    awaitConditionMet(
      s"Unhealthy nodes",
      {
        apis.forall{ a =>
          a.getSync("health").isSuccess
        }
      }, maxRetries, delay
    )
  }


  def checkSnapshot(
                                apis: Seq[APIClient],
                                num: Int = 2,
                                maxRetries: Int = 30,
                                delay: Long = 3000
                              ): Boolean = {
    awaitConditionMet(
      s"Less than $num snapshots",
      {
        apis.forall{ a =>
          val m = a.metrics
          val info = a.getBlocking[SnapshotInfo]("info")
          info.snapshot.checkpointBlocks.nonEmpty && info.acceptedCBSinceSnapshot.nonEmpty &&
          m.get("snapshotCount").exists{_.toInt > num}
        }
      }, maxRetries, delay
    )
  }



  def awaitConditionMet(
                         err: String,
                         t : => Boolean,
                         maxRetries: Int = 10,
                         delay: Long = 2000
                       ): Boolean = {

    var retries = 0
    var done = false

    do {
      retries += 1
      done = t
      logger.info(s"$err Waiting ${delay/1000} sec. Num attempts: $retries out of $maxRetries")
      Thread.sleep(delay)
    } while (!done && retries < maxRetries)
    if (!done) logger.error(s"$err TIME EXCEEDED")
    assert(done)
    done
  }

  def awaitCheckpointsAccepted(
                                apis: Seq[APIClient],
                                numAccepted: Int = 20,
                                maxRetries: Int = 20,
                                delay: Long = 3000
                              ): Boolean = {
    awaitConditionMet(
      s"Accepted checkpoints below $numAccepted",
      {
        apis.forall{ a =>
          val maybeString = a.metrics.get("checkpointAccepted")
          maybeString.exists(_.toInt > numAccepted)
        }
      }, maxRetries, delay
    )
  }

  def sendRandomTransaction(apis: Seq[APIClient]): Future[HttpResponse[String]] = {
    val src = randomNode(apis)
    val dst = randomOtherNode(src, apis).id.address.address

    val s = SendToAddress(dst, Random.nextInt(1000).toLong)
    src.post("sendTransactionToAddress", s)
  }

  def triggerRandom(apis: Seq[APIClient]): Unit = {
    apis.foreach(_.postEmpty("random"))
  }
  def setReady(apis: Seq[APIClient]): Unit = {
    apis.foreach(_.postEmpty("ready"))
  }

  def run(
           attemptSetExternalIP: Boolean = false,
           apis: Seq[APIClient],
           peerApis: Seq[APIClient]
         )(implicit executionContext: ExecutionContext): Boolean = {

    assert(checkHealthy(apis))
    logger.info("Health validation passed")

    setIdLocal(apis)

    if (attemptSetExternalIP) {
      assert(setExternalIP(apis))
    }

    addPeers(apis, peerApis)
    assert(checkPeersHealthy(apis))
    logger.info("Peer validation passed")

    val goe = genesis(apis)
    apis.foreach{_.post("genesis/accept", goe)}

    assert(checkGenesis(apis))
    logger.info("Genesis validation passed")

    triggerRandom(apis)

    setReady(apis)

    assert(awaitCheckpointsAccepted(apis))

    logger.info("Checkpoint validation passed")

    assert(checkSnapshot(apis))

    logger.info("Snapshot validation passed")

    true

  }

}
