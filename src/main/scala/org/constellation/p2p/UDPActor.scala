package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.util.{ByteString, Timeout}
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import org.constellation.consensus.Consensus.RemoteMessage

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.util.Random
import org.constellation.serializer.KryoSerializer._
import constellation._

// Consider adding ID to all UDP messages? Possibly easier.
case class UDPMessage(data: Any, remote: InetSocketAddress)
case class GetUDPSocketRef()
case class UDPSend[T <: RemoteMessage](data: T, remote: InetSocketAddress)
case class RegisterNextActor(nextActor: ActorRef)
case class GetSelfAddress()
case class Ban(address: InetSocketAddress)
case class GetBanList()

case object GetPacketGroups

// Need to catch alert messages to detect socket closure.
class UDPActor(@volatile var nextActor: Option[ActorRef] = None,
               port: Int = 16180,
               bindInterface: String = "0.0.0.0") extends Actor {

  import context.system

  private val address = new InetSocketAddress(bindInterface, port)

  IO(Udp) ! Udp.Bind(self, address, List(
    Udp.SO.ReceiveBufferSize(1024 * 1024 * 20),
    Udp.SO.SendBufferSize(1024 * 1024 * 20),
    Udp.SO.ReuseAddress.apply(true))
  )

  @volatile var udpSocket: ActorRef = _

  // TODO: save to disk
  @volatile var bannedIPs: Seq[InetSocketAddress] = Seq.empty[InetSocketAddress]

  implicit val timeout: Timeout = Timeout(10, TimeUnit.SECONDS)

  private val packetGroups = TrieMap[Long, TrieMap[Int, SerializedUDPMessage]]()

  import constellation._

  def receive: PartialFunction[Any, Unit] = {
    case Udp.Bound(_) =>
      val ref = sender()
      udpSocket = ref
      context.become(ready(ref))
    case RegisterNextActor(next) =>
      nextActor = Some(next)
  }

  def processMessage(d: Any, remote: InetSocketAddress): Unit = {
    nextActor.foreach { n => n ! UDPMessage(d, remote) }
  }

  def ready(socket: ActorRef): Receive = {

    case GetPacketGroups => sender() ! packetGroups

    case Udp.Received(data, remote) =>

      if (bannedIPs.contains(remote)) {
        println(s"BANNED MESSAGE DETECTED FROM $remote")
      } else {

        val byteArray = data.toArray

        val serMsg = deserialize(byteArray).asInstanceOf[SerializedUDPMessage]

        val pg = serMsg.packetGroup

        def updatePacketGroup(serMsg: SerializedUDPMessage, messages: TrieMap[Int, SerializedUDPMessage]): Unit = {

          println("Update packet group")
          // make sure this is not a duplicate packet first
          if (!messages.isDefinedAt(serMsg.packetGroupId)) {

            if (messages.keySet.size + 1 == serMsg.packetGroupSize) {

              messages += (serMsg.packetGroupId -> serMsg)

              val message = deserializeGrouped(messages.values.toList)

              println(s"Received BULK UDP message from $remote -- $message -- sending to $nextActor")

              processMessage(message, remote)

              packetGroups.remove(pg)

            } else {
              packetGroups(pg) = messages += (serMsg.packetGroupId -> serMsg)
            }
          }

        }

        packetGroups.get(pg) match {

          case Some(messages) =>
            updatePacketGroup(serMsg, messages)

          case None =>
            updatePacketGroup(serMsg, TrieMap())

        }
      }

    case UDPSend(data, remote) =>
      val ser: Seq[SerializedUDPMessage] = serializeGrouped(data)

      ser.foreach{ s: SerializedUDPMessage => {
        val byteString = ByteString(serialize(s))
        socket ! Udp.Send(byteString, remote)
      }}

    case RegisterNextActor(next) => nextActor = Some(next)

    case Ban(remote) =>
      // bannedIPs = {bannedIPs ++ Seq(remote)}.distinct

    case GetBanList => sender() ! bannedIPs

    case GetUDPSocketRef => sender() ! udpSocket

    case GetSelfAddress => sender() ! address

    case Udp.Unbind => socket ! Udp.Unbind

    case Udp.Unbound => context.stop(self)

  }

}

// Change packetGroup to UUID
case class SerializedUDPMessage(data: ByteString,
                                packetGroup: Long,
                                packetGroupSize: Long,
                                packetGroupId: Int) extends RemoteMessage
