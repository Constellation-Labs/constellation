package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.util.{ByteString, Timeout}

import org.constellation.DAO
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.serializer.KryoSerializer._

import scala.collection.concurrent.TrieMap

// Consider adding ID to all UDP messages? Possibly easier.

/** User Datagram Protocol Message ??. */
case class UDPMessage(data: Any, remote: InetSocketAddress)

// doc
case class GetUDPSocketRef()

/** User Datagram Protocol sendout ??. */
case class UDPSend[T <: RemoteMessage](data: T, remote: InetSocketAddress)

// doc
case class RegisterNextActor(nextActor: ActorRef)

// doc
case class GetSelfAddress()

/** Address ban. */
case class Ban(address: InetSocketAddress)

// doc
case class GetBanList()

case object GetPacketGroups

// Need to catch alert messages to detect socket closure.

/** User Datagram Protocol Actor. */
class UDPActor(@volatile var nextActor: Option[ActorRef] = None,
               port: Int = 16180,
               bindInterface: String = "0.0.0.0",
               dao: DAO) extends Actor {

  import context.system

  private val address = new InetSocketAddress(bindInterface, port)

  IO(Udp) ! Udp.Bind(self, address, List(
    Udp.SO.ReceiveBufferSize(1024 * 1024 * 20),
    Udp.SO.SendBufferSize(1024 * 1024 * 20),
    Udp.SO.ReuseAddress.apply(true))
  )

  @volatile var udpSocket: ActorRef = _

  implicit val timeout: Timeout = Timeout(10, TimeUnit.SECONDS)

  private val packetGroups = TrieMap[Long, TrieMap[Int, SerializedUDPMessage]]()

  /** Receive actor method. */
  def receive: PartialFunction[Any, Unit] = {
    case Udp.Bound(_) =>
      val ref = sender()
      udpSocket = ref
      context.become(ready(ref))
    case RegisterNextActor(next) =>
      nextActor = Some(next)
  }

  /** Send messages??. */
  def processMessage(d: Any, remote: InetSocketAddress): Unit = {
    nextActor.foreach { n => n ! UDPMessage(d, remote) }
  }

  /** Ready actor method. */
  def ready(socket: ActorRef): Receive = {

    case GetPacketGroups => sender() ! packetGroups

    case Udp.Received(data, remote) =>

      if (dao.bannedIPs.contains(remote)) {
        println(s"BANNED MESSAGE DETECTED FROM $remote")
      } else {

        val byteArray = data.toArray

        val serMsg = deserialize(byteArray).asInstanceOf[SerializedUDPMessage]

        val pg = serMsg.packetGroup

        /** Helper. @todo: Documentation. */
        def updatePacketGroup(serMsg: SerializedUDPMessage, messages: TrieMap[Int, SerializedUDPMessage]): Unit = {

          // make sure this is not a duplicate packet first
          if (!messages.isDefinedAt(serMsg.packetGroupId)) {

            if (messages.keySet.size + 1 == serMsg.packetGroupSize) {

              messages += (serMsg.packetGroupId -> serMsg)

              val message = deserializeGrouped(messages.values.toList)

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

      ser.foreach { s: SerializedUDPMessage => {
        val byteString = ByteString(serialize(s))
        socket ! Udp.Send(byteString, remote)
      }
      }

    case RegisterNextActor(next) => nextActor = Some(next)

    case Ban(remote) => {
      dao.bannedIPs = {
        dao.bannedIPs ++ Seq(remote)
      }.distinct
    }

    case GetUDPSocketRef => sender() ! udpSocket

    case GetSelfAddress => sender() ! address

    case Udp.Unbind => socket ! Udp.Unbind

    case Udp.Unbound => context.stop(self)

  } // end ready

} // end class UDPActor

// TODO: Change packetGroup to UUID // tmp comment

/** UDP message. */
case class SerializedUDPMessage(data: ByteString,
                                packetGroup: Long,
                                packetGroupSize: Long,
                                packetGroupId: Int) extends RemoteMessage
