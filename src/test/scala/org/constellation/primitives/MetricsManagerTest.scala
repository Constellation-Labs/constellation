package org.constellation.primitives

import java.security.KeyPair
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import akka.testkit.{TestActorRef, TestKit}
import org.constellation.DAO
import akka.actor.{ActorRef, ActorSystem, Props}
import better.files._
import com.typesafe.scalalogging.Logger
import constellation._
import io.prometheus.client.CollectorRegistry
import org.constellation.crypto.KeyUtils
import org.constellation.util.Heartbeat
import scala.util.{Failure, Try}

import io.prometheus.client.Collector.MetricFamilySamples

class MetricsManagerTest ()
  extends TestKit(ActorSystem("ConstellationTest"))
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll {

  val logger = Logger("ConstellationTest")
  logger.info("MetricsManagerTest init")

  override def afterAll: Unit = {
    logger.info("Shutting down the Actor under test")
    shutdown(system)
  }

  logger.info("Initializing the DAO actor")
  implicit val dao: DAO = new DAO()
  dao.updateKeyPair(getKeyPair)
  dao.idDir.createDirectoryIfNotExists(createParents = true)
  dao.preventLocalhostAsPeer = false
  dao.externalHostString = ""
  dao.externlPeerHTTPPort = 0
  val heartBeat: ActorRef = system.actorOf(
    Props(new Heartbeat()), "Heartbeat_Test"
  )
  dao.heartbeatActor = heartBeat
  logger.info("DAO actor initialized")

  val metricsManager = TestActorRef(Props(new MetricsManager()), "MetricsManager_Test")
  dao.metricsManager = metricsManager
  logger.info("MetricsManager actor initialized")

  "MetricsManager" should "report micrometer metrics" in {
    val metricSample: MetricFamilySamples = CollectorRegistry.defaultRegistry.metricFamilySamples().nextElement()
    assert(!metricSample.name.isEmpty)
  }

  private def getKeyPair = {
    val keyPairPath = ".dag/key"
    val localKeyPair = Try {
      File(keyPairPath).lines.mkString.x[KeyPair]
    }
    localKeyPair match {
      case Failure(e) =>
        e.printStackTrace()
      case _ =>
    }
    // TODO: update to take from config
    val keyPair: KeyPair = {
      localKeyPair.getOrElse {
        logger.info(s"Key pair not found in $keyPairPath - Generating new key pair")
        val kp = KeyUtils.makeKeyPair()
        File(keyPairPath).write(kp.json)
        kp
      }
    }
    keyPair
  }
}