package org.constellation.p2p

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.p2p.PeerToPeer._
import org.constellation.wallet.KeyUtils
import org.scalatest._

class PeerToPeerTest extends TestKit(ActorSystem("BlockChain")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  trait WithPeerToPeerActor {
    val keyPair: KeyPair = KeyUtils.makeKeyPair()

    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

    val peerToPeerActor: ActorRef =
      system.actorOf(Props(new PeerToPeer(keyPair.getPublic, system)(timeout)))
  }

  "A PeerToPeer actor " should " start with an empty set of peers" in new WithPeerToPeerActor {
      peerToPeerActor ! GetPeers
      expectMsg(Peers(Nil))
  }

  it should "register new peers" in new WithPeerToPeerActor {
    val probe = TestProbe()
    peerToPeerActor ! PeerRef(probe.ref)
    peerToPeerActor ! GetPeers

    expectMsgPF() {
      case Peers(Seq(address)) => address shouldEqual(probe.ref.path.toSerializationFormat)
    }
  }

  it should "add us as a peer when we send a handshake" in new WithPeerToPeerActor {
    peerToPeerActor ! HandShake
    peerToPeerActor ! GetPeers

    expectMsgPF() {
      case Peers(Seq(peer)) => peer should include("testActor")
    }
  }

  it should "handle a list of peers by adding them one by one and broadcasting to the original peers" in new WithPeerToPeerActor {

    Given("an initial peer")
    val peerProbe = TestProbe()

    peerToPeerActor ! AddPeer(peerProbe.ref.path.toStringWithoutAddress)
    peerProbe.expectMsg(HandShake)
    peerProbe.expectMsg(GetPeers)

    When("we register 2 new peers")
    val probes = Seq(TestProbe(), TestProbe()).map(_.ref.path.toSerializationFormat)
    peerToPeerActor ! Peers(probes)

    Then("the original peer should receive a notification for each one")
    peerProbe.expectMsg(AddPeer(probes(0)))
    peerProbe.expectMsg(AddPeer(probes(1)))

  }

}
