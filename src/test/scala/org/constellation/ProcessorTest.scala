package org.constellation

import java.security.KeyPair

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestProbe
import org.constellation.Fixtures.{addPeerRequest, dummyTx, id}
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.PeerData
import org.constellation.primitives.Schema.{Id, Transaction}
import org.constellation.util.APIClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.OneInstancePerTest

import scala.concurrent.ExecutionContext

trait ProcessorTest extends MockFactory with OneInstancePerTest {
  implicit val system: ActorSystem = ActorSystem("ProcessorTest")
  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val executionContext = ExecutionContext.global

  val keyPair: KeyPair = KeyUtils.makeKeyPair()
  val peerData = PeerData(addPeerRequest, getAPIClient("", 1))
  val peerManager = TestProbe()
  val metricsManager = TestProbe()
  val dbActor = TestProbe()
  val mockData = new DAO
  mockData.updateKeyPair(keyPair)
  val mockKVDB = new KVDBImpl(mockData)

  val data = makeDao(mockData)
  val tx: Transaction = dummyTx(data)
  val tx2: Transaction = dummyTx(data, 3L)
  val invalidTx = dummyTx(data, -1L)
  val srcHash = tx.src.hash
  val txHash = tx.hash
  val invalidSpendHash = invalidTx.hash
  val randomPeer: (Id, PeerData) = (id, peerData)

  def makeDao(mockData: DAO, peerManager: TestProbe = peerManager, metricsManager: TestProbe = metricsManager,
              dbActor: TestProbe = dbActor) = {
    mockData.actorMaterializer = materialize
    mockData.dbActor = stub[KVDB]
    mockData.metricsManager = metricsManager.testActor
    mockData.peerManager = peerManager.testActor
    mockData
  }


  def getAPIClient(hostName: String, httpPort: Int) = {
    val api = new APIClient(hostName, httpPort)
    api.id = id
    api
  }
}
