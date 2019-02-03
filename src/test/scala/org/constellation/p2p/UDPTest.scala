package org.constellation.p2p

import java.security.PublicKey
import java.util.concurrent.TimeUnit
import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout

import org.constellation.primitives.Schema.Id

import org.scalatest._

/** Documentation. */
case class AnotherPublicKey(c: Id, seq: Seq[PublicKey])

/** Documentation. */
case class TestManyPublicKeys(a: PublicKey, b: Seq[PublicKey], d: AnotherPublicKey)

/** Documentation. */
class UDPTest extends TestKit(ActorSystem("UDP")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

  /** Documentation. */
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  // UDP Actor
  /*
  "UDP Test" should "send UDP packets and receive them properly" in {

    val rx1 = TestProbe()
    val rx2 = TestProbe()

    val rPort1 = scala.util.Random.nextInt(50000) + 5000
    val rPort2 = scala.util.Random.nextInt(50000) + 5000

    val listener1 = system.actorOf(Props(new UDPActor(Some(rx1.ref), rPort1, dao = new Data())), s"listener1")

    val listener2 = system.actorOf(Props(new UDPActor(Some(rx2.ref), rPort2, dao = new Data())), s"listener2")

    val testMessage = Gossip(TestHelpers.createTestBundle())

    val testMessageBytes = KryoSerializer.serialize(testMessage)

    listener1 ! UDPSend(testMessage, new InetSocketAddress("localhost", rPort2))

//    rx2.expectMsg(UDPMessage(SerializedUDPMessage(ByteString(testMessageBytes), 1, 1, 1), new InetSocketAddress("localhost", rPort1)))

    Thread.sleep(4000)

//    .expectMsg(UDPSend(data, new InetSocketAddress("localhost", rPort1)))

    /*
    import akka.pattern.ask

    assert((rx1 ? "done?").mapTo[Boolean].get())
    assert((rx2 ? "done?").mapTo[Boolean].get())

    listener1 ! Udp.Unbind
    listener2 ! Udp.Unbind

    Thread.sleep(100)
    */
  }
  */

/*

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
    val rx1 = TestProbe()

    val rPort1 = scala.util.Random.nextInt(50000) + 5000
    val listener1 = system.actorOf(Props(new UDPActor(Some(rx1.ref), rPort1)), s"listener12")

    Thread.sleep(100)

    import constellation._

    val data = TestMessage("a"*1000, 1)

    listener1 ! UDPSend(data,  new InetSocketAddress("localhost", rPort1))

    Thread.sleep(1000)

    import akka.pattern.ask

    val pgs = (listener1 ? GetPacketGroups).mapTo[scala.collection.mutable.HashMap[Long, Seq[SerializedUDPMessage]]].get()

    val messages = pgs.values.flatten.toSeq.sortBy(_.packetGroupId)

    /*
    messages.zip(ser).foreach{
      case (m1, m2) =>
        assert(m1.data.sameElements(m2.data))
    }
    */

    Thread.sleep(700)

  //  assert((rx1 ? "done?").mapTo[Boolean].get())
  //  assert((rx1 ? "msg").mapTo[TestMessage].get() == data)

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

  */

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
  }
  */

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

