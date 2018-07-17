package org.constellation.util

import java.util.concurrent.ForkJoinPool

import akka.http.scaladsl.model.StatusCodes
import org.constellation.primitives.Schema._
import constellation._

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Random, Try}


class Simulation(apis: Seq[APIClient]) {

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(new ForkJoinPool(100))

  def healthy(): Boolean = apis.forall{ a => a.getBlockingStr[String]("health", timeout = 100) == "OK"}
  def setIdLocal(): Unit = apis.foreach{ a =>
    val id = a.getBlocking[Id]("id")
    a.id = id
  }
  def setExternalIP(): Boolean =
    apis.forall{a => a.postSync("ip", a.host + ":" + a.udpPort).status == StatusCodes.OK}

  def verifyGenesisReceived(): Boolean = {
    apis.forall { a =>
      val gbmd = a.getBlocking[Metrics]("metrics")
      gbmd.metrics("numValidBundles").toInt >= 1
    }
  }

  def genesis(): Unit = {
    val r1 = apis.head
    // Create a genesis transaction
    val numCoinsInitial = 4e9.toLong
    val genTx = r1.getBlocking[Transaction]("genesis/" + numCoinsInitial)
    Thread.sleep(2000)
    val gbmd = r1.getBlocking[Metrics]("metrics")
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

  def addPeers(): Seq[Future[Unit]] = {
    val results = apis.flatMap { a =>
      val ip = a.host
      println(s"Trying to add nodes to $ip")
      val others = apis.filter {_.id != a.id}.map { z => z.host + ":" + z.udpPort}
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

  def verifyPeersAdded(): Boolean = apis.forall { api =>
    val peers = api.getBlocking[Seq[Peer]]("peerids")
    println("Peers length: " + peers.length)
    peers.length == (apis.length - 1)
  }

  def assignReputations(): Unit = apis.foreach{ api =>
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

  def initialDistributionTX(): Seq[Transaction] = {

    println("-"*10)
    println("Initial distribution")
    println("-"*10)

    apis.tail.map{ n =>
      val dst = n.id.address.address
      val s = SendToAddress(dst, 1e7.toLong)
      apis.head.postRead[Transaction]("sendToAddress", s)
    }
  }


  def randomNode = apis(Random.nextInt(apis.length))
  def randomOtherNode(not: APIClient): APIClient = apis.filter{_ != not}(Random.nextInt(apis.length - 1))

  def sendRandomTransaction: Future[Transaction] = {
    Future {
      val src = randomNode
      val dst = randomOtherNode(src).id.address.address
      val s = SendToAddress(dst, Random.nextInt(1000).toLong)
      src.postRead[Transaction]("sendToAddress", s)
    }(ec)
  }

  def sendRandomTransactions(numTX: Int = 20): Set[Transaction] = {

    val txResponse = Seq.fill(numTX) {
      sendRandomTransaction
    }

    val txResponseFut = Future.sequence(txResponse)
    val txs = txResponseFut.get(100).toSet
    txs
  }

  def validateRun(txSent: Set[Transaction], validationFractionAcceptable: Double): Boolean = {

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
//      println("Num valid TX hashes " + validTXs.map{_.size})
      println("Pct complete " + pctComplete.map{a =>  (a*100).toString.slice(0, 4) + "%"})

      // This is just used to ensure processing continues
      if (attempts % 2 == 0) sendRandomTransaction

    }

    done
  }

  def nonEmptyBalance(): Boolean = apis.forall(_.getBlockingStr("balance").toLong > 0L)

  var healthChecks = 0

  def run(validationFractionAcceptable: Double = 1.0, attemptSetExternalIP: Boolean = true): Unit = {

    while (healthChecks < 10) {
      if (Try{healthy()}.getOrElse(false)) {
        healthChecks = Int.MaxValue
      } else {
        healthChecks += 1
        println(s"Unhealthy nodes. Waiting 30s. Num attempts: $healthChecks out of 10")
        Thread.sleep(30000)

      }
    }

    assert(healthy())
    setIdLocal()
    if (attemptSetExternalIP) {
      assert(setExternalIP())
    }
    genesis()

    val results = addPeers()
    import scala.concurrent.duration._
    Await.result(Future.sequence(results), 60.seconds)

    assert(verifyPeersAdded())

    Thread.sleep(2000)

    assert(verifyGenesisReceived())

    initialDistributionTX()

    Thread.sleep(15000)

//    assert(nonEmptyBalance())

    val start = System.currentTimeMillis()

    val txs = sendRandomTransactions()

    assert(validateRun(txs, validationFractionAcceptable))

    val end = System.currentTimeMillis()

    println(s"Completion time seconds: ${(end-start) / 1000}")

  }

}
