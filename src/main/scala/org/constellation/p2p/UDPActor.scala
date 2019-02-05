package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.util.{ByteString, Timeout}
import scala.collection.concurrent.TrieMap

import org.constellation.DAO
import org.constellation.serializer.KryoSerializer._

/**
  * None of this code is used right now
  * We will revisit it in the future as alternative to REST/TCP
  */

// Consider adding ID to all UDP messages? Possibly easier.

case class UDPMessage(data: Any, remote: InetSocketAddress)

case class GetUDPSocketRef()

case class UDPSend[T](data: T, remote: InetSocketAddress)

case class RegisterNextActor(nextActor: ActorRef)

case class GetSelfAddress()

case class Ban(address: InetSocketAddress)

case class GetBanList()

case object GetPacketGroups

// Need to catch alert messages to detect socket closure.

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

      if (true) { //dao.bannedIPs.contains(remote)) {
        println(s"BANNED MESSAGE DETECTED FROM $remote")
      } else {

        val byteArray = data.toArray

        val serMsg = deserialize(byteArray).asInstanceOf[SerializedUDPMessage]

        val pg = serMsg.packetGroup

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

      ser.foreach{ s: SerializedUDPMessage => {
        val byteString = ByteString(serialize(s))
        socket ! Udp.Send(byteString, remote)
      }}

    case RegisterNextActor(next) => nextActor = Some(next)

    case Ban(remote) => {
    //  dao.bannedIPs = {dao.bannedIPs ++ Seq(remote)}.distinct
    }

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
                                packetGroupId: Int)
