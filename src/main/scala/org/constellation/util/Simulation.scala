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
    ExecutionContext.fromExecutorService(new ForkJoinPool(100))

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

  def genesisOE(apis: Seq[APIClient]): GenesisObservation = {
    val ids = apis.map{_.id}
    apis.head.postBlocking[GenesisObservation]("genesis/create", ids.tail.toSet)
  }

  def genesis(apis: Seq[APIClient]): Unit = {
    val r1 = apis.head

    // Create a genesis transaction
    val numCoinsInitial = 4e9.toLong
    val genTx = r1.getBlocking[TransactionV1]("genesis/" + numCoinsInitial)

    Thread.sleep(2000)

    val gbmd = r1.getBlocking[MetricsResult]("metrics")

    assert(gbmd.metrics("numValidBundles").toInt >= 1)

    // JSON parsing error, needs to be fixed
    /*
    val gbmd = r1.getBlocking[Seq[BundleMetaData]]("bundles")
    assert(gbmd.size == 1)
    assert(gbmd.head.height.get == 0)
    val genesisBundle = gbmd.head.bundle
    assert(genesisBundle.extractIds.head == r1.id)
    assert(genesisBundle.extractTXHash.size == 1)
    assert(gbmd.head.totalScore.nonEmpty)*/
  }

  def addPeers(apis: Seq[APIClient]): Seq[Future[Unit]] = {
    val results = apis.flatMap { a =>
      val ip = a.hostName
      println(s"Trying to add nodes to $ip")
      val others = apis.filter {_.id != a.id}.map { z => z.hostName + ":" + z.udpPort}
      others.map {
        n =>
          Future {
            val res = a.postSync("peer", n)
            println(s"Tried to add peer $n to $ip res: $res")
          }
      }
    }
    results
  }

  def addPeersV2(apis: Seq[APIClient]): Seq[Future[Unit]] = {
    val results = apis.flatMap { a =>
      val ip = a.hostName
      logger.info(s"Trying to add nodes to $ip")
      val others = apis.filter {_.id != a.id}.map { z => AddPeerRequest(z.hostName, z.udpPort, z.apiPort, z.id)}
      others.map {
        n =>
          Future {
            val res = a.postSync("addPeerV2", n)
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

  def initialDistributionTX(apis: Seq[APIClient]): Seq[TransactionV1] = {

    println("-"*10)
    println("Initial distribution")
    println("-"*10)

    apis.map{ n =>
      val dst = n.id.address.address
      val s = SendToAddress(dst, 1e7.toLong)
      apis.head.postBlocking[TransactionV1]("sendToAddress", s)
    }
  }

  def randomNode(apis: Seq[APIClient]) = apis(Random.nextInt(apis.length))

  def randomOtherNode(not: APIClient, apis: Seq[APIClient]): APIClient = apis.filter{_ != not}(Random.nextInt(apis.length - 1))

  def sendRandomTransaction(apis: Seq[APIClient]): Future[TransactionV1] = {
    Future {
      val src = randomNode(apis)
      val dst = randomOtherNode(src, apis).id.address.address
      val s = SendToAddress(dst, Random.nextInt(1000).toLong)
      src.postBlocking[TransactionV1]("sendToAddress", s)
    }(ec)
  }

  def sendRandomTransactionV2(apis: Seq[APIClient]): Future[HttpResponse[String]] = {
    val src = randomNode(apis)
    val dst = randomOtherNode(src, apis).id.address.address

    val s = SendToAddress(dst, Random.nextInt(1000).toLong)
    src.post("sendV2", s)
  }

  def sendRandomTransactions(numTX: Int = 20, apis: Seq[APIClient]): Set[TransactionV1] = {

    val txResponse = Seq.fill(numTX) {
      sendRandomTransaction(apis)
    }

    val txResponseFut = Future.sequence(txResponse)
    val txs = txResponseFut.get(100).toSet
    txs
  }

  def validateRun(txSent: Set[TransactionV1], validationFractionAcceptable: Double, apis: Seq[APIClient]): Boolean = {

    var done = false
    var attempts = 0
    val hashes = txSent.map{_.hash}

    while (!done && attempts < 50) {

      attempts += 1

      Thread.sleep(5000)

      val validTXs = apis.map{_.getBlocking[Seq[String]]("validTX")}

      val pctComplete = validTXs.map{ v =>
        val missingFraction = hashes.diff(v.toSet).size.toDouble / hashes.size
        val complete = 1 - missingFraction
        complete
      }

      done = pctComplete.forall(_ >= validationFractionAcceptable)

      println("Pct complete " + pctComplete.map{a =>  (a*100).toString.slice(0, 4) + "%"})

      // This is just used to ensure processing continues
      // TODO: remove this temporary requirement
      if (attempts % 2 == 0) sendRandomTransaction(apis)
    }

    done
  }

  def nonEmptyBalance(apis: Seq[APIClient]): Boolean = {
    apis.forall(f => {
      val balance = f.getBlockingStr("balance").toLong

      balance > 0L
    })
  }

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

  def runV2(attemptSetExternalIP: Boolean = false, apis: Seq[APIClient]): Unit = {

    awaitHealthy(apis)

    setIdLocal(apis)

    if (attemptSetExternalIP) {
      assert(setExternalIP(apis))
    }

    // Temporary for disabling V1 trigger
    apis.foreach(_.postEmpty("disableDownload"))

    //val results =
    addPeersV2(apis)
    //import scala.concurrent.duration._
    //Await.result(Future.sequence(results), 60.seconds)
    Thread.sleep(5000)
    assert(
      apis.forall{a =>
//        a.postEmpty()
        val res = a.postBlockingEmpty[Seq[(Id, Boolean)]]("peerHealthCheckV2")
        res.forall(_._2) && res.size == apis.size - 1
      }
    )

    val goe = genesisOE(apis)

    apis.foreach{_.post("genesis/accept", goe)}

    Thread.sleep(5000)

    apis.foreach{_.post("startRandomTX", goe).onComplete(println)}

    // assert(verifyPeersAdded())
  }

  def runHealthCheck(apis: Seq[APIClient]): Unit = {
    while (healthChecks < 10) {
      if (Try{healthy(apis)}.getOrElse(false)) {
        healthChecks = Int.MaxValue
      } else {
        healthChecks += 1
        println(s"Unhealthy nodes. Waiting 30s. Num attempts: $healthChecks out of 10")
        Thread.sleep(30000)
      }
    }

    assert(healthy(apis))
  }

  def connectNodes(attemptSetExternalIP: Boolean = true,
                   initGenesis: Boolean = true,
                   apis: Seq[APIClient]): Seq[APIClient] = {

    runHealthCheck(apis)

    setIdLocal(apis)

    if (attemptSetExternalIP) {
      assert(setExternalIP(apis))
    }

    if (initGenesis) {
      genesis(apis)
    }

    val results = addPeers(apis)

    Await.result(Future.sequence(results), 60.seconds)

    assert(verifyPeersAdded(apis))

    /*
    apis.foreach(a => {
      if (a.id != apis.head.id) {
        a.postEmpty("initializeDownload")
      }
    })
    */

    Thread.sleep(45000)

    assert(verifyGenesisReceived(apis))

    val txs = initialDistributionTX(apis)

    Thread.sleep(25000)

//    assert(nonEmptyBalance())

    apis
  }

}
