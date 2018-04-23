package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.Fixtures
import org.constellation.p2p.PeerToPeer._
import org.constellation.wallet.KeyUtils
import org.scalatest._

import scala.util.Random

class PeerToPeerTest extends TestKit(ActorSystem("BlockChain")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  private val address: InetSocketAddress = constellation.addressToSocket("localhost:16180")
  private val address2: InetSocketAddress = constellation.addressToSocket("localhost:16181")

  trait WithPeerToPeerActor {
    val keyPair: KeyPair = KeyUtils.makeKeyPair()

    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

    val consensusActor = TestProbe()

    val udpActor: ActorRef =
      system.actorOf(
        Props(new UDPActor(None)), s"ConstellationUDPActor" + Random.nextInt()
      )

    val peerToPeerActor: ActorRef =
      system.actorOf(Props(
        new PeerToPeer(keyPair.getPublic, system, consensusActor.ref, udpActor, address, keyPair)(timeout)
      ))

    udpActor ! RegisterNextActor(peerToPeerActor)


  }

  "A PeerToPeer actor " should " start with an empty set of peers" in new WithPeerToPeerActor {
      peerToPeerActor ! GetPeers
      expectMsg(Peers(Nil))
  }

  it should "register new peers" in new WithPeerToPeerActor {
    val probe = TestProbe()
    peerToPeerActor ! PeerRef(address2)
    import akka.pattern.ask
    import constellation._
    Thread.sleep(100)
    val res: Peers = (peerToPeerActor ? GetPeers).mapTo[Peers].get()
    assert(res == Peers(Seq(address2)))
  }

  it should "add us as a peer when we send a handshake" in new WithPeerToPeerActor {
    peerToPeerActor ! UDPMessage(HandShake(Id(Fixtures.publicKey), address, Peers(Seq())), address2)
    Thread.sleep(100)

    peerToPeerActor ! GetPeers

    expectMsgPF() {
      case Peers(Seq(peer)) => peer shouldEqual address2
    }
  }

  // TODO: Reimplement -- this test is already covered by multi-node and is much more complex now with UDP
  // Fix later
/*
  it should "handle a list of peers by adding them one by one and broadcasting to the original peers" in new WithPeerToPeerActor {

    Given("an initial peer")
    val peerProbe = TestProbe()

    peerToPeerActor ! AddPeerFromLocal(address2)
    peerProbe.expectMsg(HandShake)
    peerProbe.expectMsg(GetPeers)

    When("we register 2 new peers")
    val probes = Seq(address, address2)
    peerToPeerActor ! Peers(probes)

    Then("the original peer should receive a notification for each one")
    peerProbe.expectMsg(AddPeerFromLocal(probes(0)))
    peerProbe.expectMsg(AddPeerFromLocal(probes(1)))

  }
*/

}
