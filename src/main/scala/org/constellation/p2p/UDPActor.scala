package org.constellation.p2p

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.serialization.SerializationExtension
import akka.util.ByteString

case class UDPMessage(data: Any, remote: InetSocketAddress)
case class GetUDPSocketRef()
case class UDPSend(data: ByteString, remote: InetSocketAddress)
case class RegisterNextActor(nextActor: ActorRef)
case class GetSelfAddress()
case class Ban(address: InetSocketAddress)

// This is using java serialization which is NOT secure. We need to update to use another serializer
// Examples below:
// https://github.com/calvinlfer/Akka-Persistence-example-with-Protocol-Buffers-serialization/blob/master/src/main/scala/com/experiments/calculator/serialization/CalculatorEventProtoBufSerializer.scala
// https://github.com/dnvriend/akka-serialization-test/tree/master/src/main/scala/com/github/dnvriend/serializer
// Serialization below is just a temporary hack to avoid having to make more changes for now.

class UDPActor(
                var nextActor: Option[ActorRef] = None,
                port: Int = 16180,
                bindInterface: String = "0.0.0.0"
              ) extends Actor {

  import context.system

  private val address = new InetSocketAddress(bindInterface, port)
  IO(Udp) ! Udp.Bind(self, address)

  @volatile var udpSocket: ActorRef = _
  @volatile var bannedPeers: Set[InetSocketAddress] = Set.empty[InetSocketAddress]

  import constellation._

  def receive: PartialFunction[Any, Unit] = {
    case Udp.Bound(_) =>
      val ref = sender()
      udpSocket = ref
      context.become(ready(ref))
    case RegisterNextActor(next) => nextActor = Some(next)
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      if (!bannedPeers.contains(remote)) {
        val str = data.utf8String
        val serialization = SerializationExtension(context.system)
        val serMsg = str.x[SerializedUDPMessage]
        val deser = serialization.deserialize(serMsg.data, serMsg.serializer, Some(classOf[Any]))
        println(s"Received UDP message from $remote -- $deser -- sending to $nextActor")
        deser.foreach { d =>
          nextActor.foreach { n => n ! UDPMessage(d, remote) }
        }
      } else {
        println(s"BANNED MESSAGE DETECTED FROM $remote")
      }
    case Udp.Unbind => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
    case GetUDPSocketRef => sender() ! udpSocket
    case UDPSend(data, remote) => socket ! Udp.Send(data, remote)
    case RegisterNextActor(next) => nextActor = Some(next)
    case GetSelfAddress => sender() ! address
    case Ban(remote) => bannedPeers += remote
  }
}

case class SerializedUDPMessage(data: Array[Byte], serializer: Int)
