package org.constellation.util

import java.util.concurrent.ForkJoinPool

import com.typesafe.scalalogging.Logger

import scala.concurrent.duration._
import org.constellation.primitives.Schema._
import constellation._
import org.constellation.AddPeerRequest
import scalaj.http.HttpResponse

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Random, Try}

class Simulation {

  val logger = Logger(s"Simulation")

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(new ForkJoinPool(1024))

  def healthy(apis: Seq[APIClient]): Boolean = {
    apis.forall(a => {
      val res = a.getSync("health", timeoutSeconds = 100).isSuccess
      res
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

  def addPeers(apis: Seq[APIClient], peerAPIs: Seq[APIClient]): Seq[Future[Unit]] = {
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

  var healthChecks = 0

  def awaitHealthy(apis: Seq[APIClient]): Unit = {
    while (healthChecks < 10) {
      if (Try{healthy(apis)}.getOrElse(false)) {
        healthChecks = Int.MaxValue
      } else {
        healthChecks += 1
        logger.error(s"Unhealthy nodes. Waiting 30s. Num attempts: $healthChecks out of 10")
        Thread.sleep(30000)
      }
    }

    assert(healthy(apis))
  }

  def run(attemptSetExternalIP: Boolean = false, apis: Seq[APIClient], peerApis: Seq[APIClient]): Unit = {

    awaitHealthy(apis)

    setIdLocal(apis)

    if (attemptSetExternalIP) {
      assert(setExternalIP(apis))
    }

    addPeers(apis, peerApis)

    Thread.sleep(15000)

    assert(
      apis.forall{a =>
        val res = a.postBlockingEmpty[Seq[(Id, Boolean)]]("peerHealthCheck")
        res.forall(_._2) && res.size == apis.size - 1
      }
    )

    val goe = genesis(apis)

    apis.foreach{_.post("genesis/accept", goe)}
  }

}
