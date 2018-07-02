package org.constellation.p2p

import java.net.InetSocketAddress
import java.security.{KeyPair, PrivateKey, PublicKey}
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.io.Udp
import akka.serialization.SerializationExtension
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.{ByteString, Timeout}
import com.esotericsoftware.kryo.io.{Input, Output}
import constellation.kryo
import org.json4s.native.Serialization
import org.scalatest._

import scala.util.{Random, Try}
import constellation._
import org.constellation.primitives.Schema.{Id, Peer}
import org.constellation.util.{ProductHash, Signed}

case class TestMessage(a: String, b: Int) extends ProductHash

case class AnotherPublicKey(c: Id, seq: Seq[PublicKey])
case class TestManyPublicKeys(a: PublicKey, b: Seq[PublicKey], d: AnotherPublicKey)

class TestReceiveUDP extends Actor {

  var done = false
  var msg: TestMessage = _

  def receive: PartialFunction[Any, Unit] = {
    case u @ UDPMessage(t: TestMessage, remote) =>
      println(
        s"Success on ${self.path.name}! " +
          s"Receive loop got a properly decoded case class of correct type: " + u
      )
      msg = t
      done = true
    case "msg" => sender() ! msg
    case _ => sender() ! done
  }

}

class UDPTest extends TestKit(ActorSystem("UDP")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  // Initialize
  kryo

  "UDP Test" should "send UDP packets and receive them properly" in {

    val rx1 = system.actorOf(Props(new TestReceiveUDP), s"rx1")
    val rx2 = system.actorOf(Props(new TestReceiveUDP), s"rx2")

    val rPort1 = scala.util.Random.nextInt(50000) + 5000
    val rPort2 = scala.util.Random.nextInt(50000) + 5000
    val listener1 = system.actorOf(Props(new UDPActor(Some(rx1), rPort1)), s"listener1")
    val listener2 = system.actorOf(Props(new UDPActor(Some(rx2), rPort2)), s"listener2")

    Thread.sleep(100)

    import constellation._

    val data = TestMessage("a", 1)

    listener1.udpSend(data, new InetSocketAddress("localhost", rPort1))
    listener2.udpSend(data, new InetSocketAddress("localhost", rPort2))
    Thread.sleep(500)
    import akka.pattern.ask

    assert((rx1 ? "done?").mapTo[Boolean].get())
    assert((rx2 ? "done?").mapTo[Boolean].get())

    listener1 ! Udp.Unbind
    listener2 ! Udp.Unbind
    Thread.sleep(100)

  }

/*  "UDP Serialize" should "work with regular methods" in {

    val data = TestMessage("a"*10000, 1)

    import constellation._
    val msgs = data.udpSerializeGrouped()
    var deser: Any = null
    val packetGroups = scala.collection.mutable.HashMap[Long, Seq[SerializedUDPMessage]]()

    msgs.foreach{
      serMsg =>
        val pg = serMsg.packetGroup.get
        packetGroups.get(pg) match {
          case Some(messages) =>
            if (messages.length + 1 == serMsg.packetGroupSize.get) {
              // Done
              val dat = {messages ++ Seq(serMsg)}.sortBy(_.packetGroupId.get).flatMap{_.data}.toArray
           //   deser = serialization.deserialize(dat, serMsg.serializer, Some(classOf[Any])).get
              val kryoInput = new Input(dat)
              deser = kryo.readClassAndObject(kryoInput)
              packetGroups.remove(pg)
            } else {
              packetGroups(pg) = messages ++ Seq(serMsg)
            }
          case None =>
            packetGroups(pg) = Seq(serMsg)
        }
    }

    assert(deser == data)

  }

  */

  "UDP Bulk" should "send UDP packets in groups and decode properly" in {

    val rx1 = system.actorOf(Props(new TestReceiveUDP), s"rx12")

    val rPort1 = scala.util.Random.nextInt(50000) + 5000
    val listener1 = system.actorOf(Props(new UDPActor(Some(rx1), rPort1)), s"listener12")

    Thread.sleep(100)

    import constellation._

    val data = TestMessage("a"*1000, 1)

    val ser = data.udpSerializeGrouped(50)

    ser.foreach { d =>
      listener1 ! UDPSend(ByteString(d.json),  new InetSocketAddress("localhost", rPort1))
    //  listener1 ! UDPSend(ByteString(d.kryoWrite),  new InetSocketAddress("localhost", rPort1))
    }

    Thread.sleep(1000)

    import akka.pattern.ask
    val pgs = (listener1 ? GetPacketGroups).mapTo[scala.collection.mutable.HashMap[Long, Seq[SerializedUDPMessage]]].get()

    val messages = pgs.values.flatten.toSeq.sortBy(_.packetGroupId.get)

    println(s"Sizes pg: ${messages.size} original: ${ser.size}")

    messages.zip(ser).foreach{
      case (m1, m2) =>
        assert(m1.data.sameElements(m2.data))
    }


    Thread.sleep(700)

    assert((rx1 ? "done?").mapTo[Boolean].get())
    assert((rx1 ? "msg").mapTo[TestMessage].get() == data)

    listener1 ! Udp.Unbind
    Thread.sleep(100)

  }

  "InetSocketAddress serializes" should "use json for address" in {

    val addr = new InetSocketAddress("localhost", 5000)
    import constellation._
    addr.json.x[InetSocketAddress] shouldEqual addr
    addr.getHostName shouldEqual "localhost"
    addr.getHostString shouldEqual "localhost"

    // Apparently this fails on circle-ci
    // addr.getAddress.getCanonicalHostName shouldEqual "127.0.0.1"
    /*
    addr.getAddress.getHostAddress shouldEqual "127.0.0.1"
    addr.getAddress.getHostName shouldEqual "localhost"
    addr.toString shouldEqual "localhost/127.0.0.1:5000"
*/
  }

/*
  "Kryo serialize" should "work with keys" in {

    val kp = makeKeyPair()
    val prv = kp.getPrivate
    val stuff = prv.kryoWrite
    val pub = kp.getPublic
    val pubWrite = pub.kryoWrite
    assert(pubWrite.kryoRead.asInstanceOf[PublicKey] == pub)
   // assert(stuff.kryoRead.asInstanceOf[PrivateKey] == prv)

  }
*/
/*

  "Kryo serialize large class" should "work with keys" in {

    val kp = makeKeyPair()

    val pub = kp.getPublic

    val peer = Peer(Id(pub), new InetSocketAddress("localhost", 16180))

    val peerBytes = peer.kryoWrite
    val peerRead = peerBytes.kryoRead.asInstanceOf[Peer]
    assert(peerRead == peer)

    import org.constellation.Fixtures._
    val tm = TestManyPublicKeys(publicKey, Seq(publicKey1, publicKey2), AnotherPublicKey(Id(publicKey3), Seq(publicKey4)))
    val tm2 = tm.kryoWrite.kryoRead.asInstanceOf[TestManyPublicKeys]
    assert(tm == tm2)

  }

  "Kryo serialize signed" should "work with signing data" in {

    val kp = makeKeyPair()

    val pub = kp.getPublic
    // val peer = Peer(Id(pub), new InetSocketAddress("localhost", 16180))
    val tm = TestMessage("a", 1)
    val hm = tm.signed()(kp)
    val hm2 = hm.kryoWrite.kryoRead.asInstanceOf[Signed[TestMessage]]
    assert(hm2 == hm)

  }*/

/*
  // TODO: GET THIS TO WORK PROPERLY
  "Kryo serialize signed2" should "work with signing data" in {

    val kp = makeKeyPair()

    val pub = kp.getPublic
    val peer = Peer(Id(pub), new InetSocketAddress("localhost", 16180))
    val hm = peer.signed()(kp)
    val written = hm.kryoWrite
    val readin = written.kryoRead
    val asin = readin.asInstanceOf[Signed[Peer]]
    val hm2 = asin

    assert(hm2 == hm)

  }
*/



}

