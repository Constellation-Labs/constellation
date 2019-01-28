package org.constellation.primitives

import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}
import akka.testkit.{TestActorRef, TestKit}

import org.constellation.DAO

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.scalalogging.Logger
import io.prometheus.client.CollectorRegistry

import org.constellation.crypto.KeyUtils
import org.constellation.util.Heartbeat

import java.util.Collections

// doc
class MetricsManagerTest()
  extends TestKit(ActorSystem("ConstellationTest"))
    with Matchers
    with FlatSpecLike
    with BeforeAndAfterAll {

  val logger = Logger("ConstellationTest")
  logger.info("MetricsManagerTest init")

  // doc
  override def afterAll: Unit = {
    logger.info("Shutting down the Actor under test")
    shutdown(system)
  }

  logger.info("Initializing the DAO actor")
  implicit val dao: DAO = new DAO()
  dao.updateKeyPair(KeyUtils.makeKeyPair())
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
    val familySamples = Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples())
    familySamples.size() should be > 0
  }

} // end MetricsManagerTest class
