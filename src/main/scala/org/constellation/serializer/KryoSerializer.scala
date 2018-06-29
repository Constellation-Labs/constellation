package org.constellation.serializer

import java.security.PublicKey

import akka.util.ByteString
import com.twitter.chill.{IKryoRegistrar, KryoInstantiator, KryoPool, ScalaKryoInstantiator}
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.p2p.SerializedUDPMessage

import scala.util.Random

object KryoSerializer {

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool: KryoPool = KryoPool.withBuffer(guessThreads,
    new ScalaKryoInstantiator().setRegistrationRequired(false)
      .withRegistrar(new ConstellationKryoRegistrar()), 32, 1024*1024*100)

  def serializeGrouped[T <: RemoteMessage](data: T, groupSize: Int = 500): Seq[SerializedUDPMessage] = {

    val bytes: Array[Byte] = kryoPool.toBytesWithClass(data)

    val idx: Seq[(Array[Byte], Int)] = bytes.grouped(groupSize).zipWithIndex.toSeq

    val pg: Long = Random.nextLong()

    idx.map { case (b: Array[Byte], i: Int) =>
      SerializedUDPMessage(ByteString(b),
        packetGroup = pg, packetGroupSize = idx.length, packetGroupId = i)
    }
  }

  def deserializeGrouped(messages: List[SerializedUDPMessage]): AnyRef = {
    val sortedBytes = messages.sortBy(f => f.packetGroupId).flatMap(_.data).toArray

    deserialize(sortedBytes)
  }

  def serialize[T <: RemoteMessage](data: T): Array[Byte] = {
    kryoPool.toBytesWithClass(data)
  }

  def deserialize(message: Array[Byte]): AnyRef= {
    kryoPool.fromBytes(message)
  }

}
