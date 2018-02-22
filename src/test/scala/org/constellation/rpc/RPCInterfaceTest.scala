package org.constellation.rpc

import akka.actor.ActorRef
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.testkit.{TestActor, TestKitBase, TestProbe}
import com.typesafe.scalalogging.Logger
import org.constellation.Fixtures
import org.constellation.blockchain._
import org.constellation.p2p.PeerToPeer._
import org.constellation.rpc.ProtocolInterface.{FullChain, GetLatestBlock, _}
import org.scalatest.{FlatSpec, Matchers}
import Fixtures.{tx, _}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import org.constellation.blockchain.Consensus.PerformConsensus

import scala.concurrent.ExecutionContext
//import akka.http.scaladsl.unmarshalling.Unmarshaller._
class RPCInterfaceTest extends FlatSpec with ScalatestRouteTest with TestKitBase
  with Matchers {

  trait RPCInterfaceFixture extends RPCInterface {
    val testProbe = TestProbe()
    val id = "id"

    override val blockChainActor: ActorRef = testProbe.ref
    override val logger = Logger("TestLogger")
    override implicit val executionContext: ExecutionContext = ExecutionContext.global
  }

  "A route " should "get the blockchain from the blockchain actor for /blocks" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case GetChain =>
        sender ! FullChain(Nil)
        TestActor.NoAutoPilot
      }
    }

    Get("/blocks") ~> routes ~> check {
      responseAs[FullChain] shouldEqual FullChain(Nil)
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
        case GetId =>
          sender ! Id("id")
          TestActor.NoAutoPilot
      }
    }
    Get("/id") ~> routes ~> check {
      responseAs[Id] shouldEqual Id("id")
    }
  }

  it should "return the latest block for /performConsensus" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case PerformConsensus =>
        sender ! genesisBlock.toString
        TestActor.NoAutoPilot
      }
    }

    Get("/performConsensus") ~> routes ~> check {
      responseAs[String] shouldEqual genesisBlock.toString
    }
  }

  it should "add a new tx for /sendTx" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case transaction: Transaction =>
        sender ! transaction.toString
        TestActor.NoAutoPilot
    }
    }

    Post("/sendTx", HttpEntity(ContentTypes.`application/json`, jsonToString(tx))) ~> routes ~> check {
      responseAs[String] shouldEqual tx.toString
    }
  }

  it should "add a new peer for /addPeer" in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case peerAddress: AddPeer =>
        sender ! s"Added peer ${peerAddress.address}"
        TestActor.NoAutoPilot
      }
    }
    Post("/addPeer", HttpEntity(ContentTypes.`text/html(UTF-8)`, "TestPeer")) ~> routes ~> check {
      responseAs[String] shouldEqual s"Added peer TestPeer"
    }
  }

  it should "a balance should be returned for /getBalance " in new RPCInterfaceFixture {
    testProbe.setAutoPilot { (sender: ActorRef, msg: Any) => msg match {
      case GetBalance(account) =>
        sender ! s"Chain cache queried for $account"
        TestActor.NoAutoPilot
    }
    }

    Post("/getBalance", HttpEntity(ContentTypes.`text/html(UTF-8)`, "1234")) ~> routes ~> check {
      responseAs[String] shouldEqual "Chain cache queried for 1234"
    }
  }

}


