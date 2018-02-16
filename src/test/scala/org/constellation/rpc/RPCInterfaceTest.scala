package org.constellation.rpc

import akka.actor.ActorRef
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{TestActor, TestKitBase, TestProbe}
import com.typesafe.scalalogging.Logger
import org.constellation.blockchain._
import ProtocolInterface._
import org.constellation.Fixtures._
import org.constellation.p2p.PeerToPeer._
import org.scalatest.{FlatSpec, Matchers}
import org.constellation.Fixtures.{jsonToString, tx}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

class RPCInterfaceTest extends FlatSpec with ScalatestRouteTest with TestKitBase
  with Matchers {

  trait RPCInterfaceFixture extends RPCInterface {
    val testProbe = TestProbe()
    val id = "id"
    val dag = new DAG

    override val blockChainActor: ActorRef = testProbe.ref
    override val logger = Logger("TestLogger")
    override implicit val executionContext: ExecutionContext = ExecutionContext.global
  }

  "A route " should "get the blockchain from the blockchain actor for /blocks" in new RPCInterfaceFixture {

    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case GetChain =>
        sender ! FullChain(dag.globalChain)
        TestActor.NoAutoPilot
      }
    }

    Get("/blocks") ~> routes ~> check {
      responseAs[ListBuffer[CheckpointBlock]] shouldEqual ListBuffer.empty[CheckpointBlock]
    }
  }

  it should "return the latest block for /latestBlock" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case getLatestBlock: GetLatestBlock =>
        sender ! getLatestBlock
        TestActor.NoAutoPilot
      }
    }

    Get("/latestBlock") ~> routes ~> check {
      responseAs[CheckpointBlock] shouldEqual GenesisBlock
    }
  }

  it should "retrieve all peers for /peers" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {

      case GetPeers =>
        sender ! Peers(Seq("PeerOne"))
        TestActor.NoAutoPilot
      }
    }

    Get("/peers") ~> routes ~> check {
      responseAs[Peers] shouldEqual Peers(Seq("PeerOne"))
    }
  }

  it should "retrieve id for /id" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) =>
      msg match {

        case getId => sender ! Id(id)
          TestActor.NoAutoPilot
      }
    }
    Get("/id") ~> routes ~> check {
      responseAs[Id] shouldEqual Id(id)
    }
  }
  it should "add a new peer for /addPeer" in new RPCInterfaceFixture {
    Post("/addPeer", HttpEntity(ContentTypes.`text/html(UTF-8)`, "TestPeer")) ~> routes ~> check {
      testProbe.expectMsg(AddPeer("TestPeer"))
    }
  }


  it should "add a new block for /addBlock" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case transaction: Transaction =>

        sender ! transaction
        TestActor.NoAutoPilot
      }
    }

    Post("/mineBlock", HttpEntity(ContentTypes.`application/json`, jsonToString(tx))) ~> routes ~> check {
      responseAs[CheckpointBlock] shouldEqual genesisBlock
    }
  }

  it should "a balance should be returned for /getBalance " in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case GetBalance(account) =>

        sender ! "Chain cache queried"
        TestActor.NoAutoPilot
      }
    }

    Post("/getBalance", HttpEntity(ContentTypes.`text/html(UTF-8)`, "1234")) ~> routes ~> check {
      responseAs[String] shouldEqual("\"Queried balance of account 1234\"")
    }
  }

}


