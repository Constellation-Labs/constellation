package org.constellation.p2p

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.serialization.SerializationExtension
import akka.util.ByteString

case class UDPMessage(data: Any, remote: InetSocketAddress)
case class GetUDPSocketRef()
case class UDPSend(data: ByteString, remote: InetSocketAddress)


class UDPActor(
                   nextActor: ActorRef,
                   port: Int = 16180,
                   bindInterface: String = "0.0.0.0"
                 ) extends Actor {

  import context.system
  IO(Udp) ! Udp.Bind(self, new InetSocketAddress(bindInterface, port))

  var udpSocket: ActorRef = _
  import constellation._

  def receive: PartialFunction[Any, Unit] = {
    case Udp.Bound(_) =>
      val ref = sender()
      udpSocket = ref
      context.become(ready(ref))
  }

  def ready(socket: ActorRef): Receive = {
    case Udp.Received(data, remote) =>
      val str = data.utf8String
      val serialization = SerializationExtension(context.system)
      val serMsg = str.x[SerializedUDPMessage]
      val deser = serialization.deserialize(serMsg.data, serMsg.serializer, Some(classOf[Any]))
      deser.foreach { d =>
        nextActor ! UDPMessage(d, remote)
      }
    case Udp.Unbind => socket ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
    case GetUDPSocketRef => sender() ! udpSocket
    case UDPSend(data, remote) => socket ! Udp.Send(data, remote)
  }
}

case class SerializedUDPMessage(data: Array[Byte], serializer: Int)
