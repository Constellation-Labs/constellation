package org.constellation.p2p

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Udp}
import akka.util.{ByteString, Timeout}
import org.constellation.consensus.Consensus.RemoteMessage

import scala.collection.mutable

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

  private val packetGroups = mutable.HashMap[Long, mutable.HashMap[Int, SerializedUDPMessage]]()

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

        val serMsg = kryoPool.fromBytes(byteArray, classOf[SerializedUDPMessage])

        this.synchronized {

            val pg = serMsg.packetGroup

            packetGroups.get(pg) match {

              case Some(messages) =>

                // make sure this is not a duplicate packet first
                if (!messages.isDefinedAt(serMsg.packetGroupId)) {

                  if (messages.keySet.size + 1 == serMsg.packetGroupSize) {

                    messages += (serMsg.packetGroupId -> serMsg)

                    val dat = messages.values.toList.sortBy(_.packetGroupId).flatMap{_.data}.toArray

                    val deser = Some(kryoPool.fromBytes(dat, classOf[Any]))

                    println(s"Received BULK UDP message from $remote -- $deser -- sending to $nextActor")

                    deser.foreach {processMessage(_, remote)}

                    packetGroups.remove(pg)

                  } else {
                    packetGroups(pg) = messages += (serMsg.packetGroupId -> serMsg)
                  }
                }

              case None =>
                packetGroups(pg) = mutable.HashMap(serMsg.packetGroupId -> serMsg)

            }
        }
      }

    case UDPSend(data, remote) =>
      val ser: Seq[SerializedUDPMessage] = udpSerializeGrouped(data)

      ser.foreach{ s: SerializedUDPMessage => {
        socket ! Udp.Send(ByteString(kryoPool.toBytesWithClass(s)), remote)
      }}

    case RegisterNextActor(next) => nextActor = Some(next)

    case Ban(remote) => bannedIPs = {bannedIPs ++ Seq(remote)}.distinct

    case GetBanList => sender() ! bannedIPs

    case GetUDPSocketRef => sender() ! udpSocket

    case GetSelfAddress => sender() ! address

    case Udp.Unbind => socket ! Udp.Unbind

    case Udp.Unbound => context.stop(self)

  }

}

// Change packetGroup to UUID
case class SerializedUDPMessage(data: ByteString,
                                typeClassName: String,
                                packetGroup: Long,
                                packetGroupSize: Long,
                                packetGroupId: Int)
