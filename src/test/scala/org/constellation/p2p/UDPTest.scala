package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.io.Udp
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.json4s.native.Serialization
import org.scalatest._

case class TestMessage(a: String, b: Int)

class TestReceiveUDP extends Actor {

  var done = false

  def receive: PartialFunction[Any, Unit] = {
    case u @ UDPMessage(t: TestMessage, remote) =>
      println(
        s"Success on ${self.path.name}! " +
          s"Receive loop got a properly decoded case class of correct type: " + u
      )
      done = true
    case _ => sender() ! done
  }

}

class UDPTest extends TestKit(ActorSystem("UDP")) with FlatSpecLike
  with ImplicitSender with GivenWhenThen with BeforeAndAfterAll with Matchers {

  implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "UDP Test" should "send UDP packets and receive them properly" in {

    val rx1 = system.actorOf(Props(new TestReceiveUDP), s"rx1")
    val rx2 = system.actorOf(Props(new TestReceiveUDP), s"rx2")
    val listener1 = system.actorOf(Props(new UDPActor(Some(rx1), 16180)), s"listener1")
    val listener2 = system.actorOf(Props(new UDPActor(Some(rx2), 16181)), s"listener2")

    Thread.sleep(100)

    import constellation._

    val data = TestMessage("a", 1)

    listener1.udpSend(data, new InetSocketAddress("localhost", 16181))
    listener2.udpSend(data, new InetSocketAddress("localhost", 16180))
    Thread.sleep(100)
    import akka.pattern.ask

    assert((rx1 ? "done?").mapTo[Boolean].get())
    assert((rx2 ? "done?").mapTo[Boolean].get())

    listener1 ! Udp.Unbind
    listener2 ! Udp.Unbind
    Thread.sleep(100)

  }

  "InetSocketAddress serializes" should "use json for address" in {

    val addr = new InetSocketAddress("localhost", 5000)
    import constellation._
    addr.json.x[InetSocketAddress] shouldEqual addr
    addr.getHostName shouldEqual "localhost"
    addr.getHostString shouldEqual "localhost"
    addr.getAddress.getCanonicalHostName shouldEqual "127.0.0.1"
    addr.getAddress.getHostAddress shouldEqual "127.0.0.1"
    addr.getAddress.getHostName shouldEqual "localhost"
    addr.toString shouldEqual "localhost/127.0.0.1:5000"

  }



}

