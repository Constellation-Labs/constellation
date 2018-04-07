package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
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
    val listener1 = system.actorOf(Props(new UDPActor(rx1, 16180)), s"listener1")
    val listener2 = system.actorOf(Props(new UDPActor(rx2, 16181)), s"listener2")

    Thread.sleep(100)

    import constellation._

    val data = TestMessage("a", 1)

    listener1.udpSend(data, new InetSocketAddress("localhost", 16181))
    listener2.udpSend(data, new InetSocketAddress("localhost", 16180))
    Thread.sleep(100)
    import akka.pattern.ask

    assert((rx1 ? "done?").mapTo[Boolean].get())
    assert((rx2 ? "done?").mapTo[Boolean].get())

  }



}

