import java.net.InetSocketAddress
import java.nio.{ByteBuffer, ByteOrder}
import java.security.{KeyPair, PrivateKey, PublicKey}
import java.util.concurrent.TimeUnit

import cats.effect.Sync
import cats.implicits._
import com.google.common.hash.Hashing
import org.constellation.DAO
import org.constellation.keytool.KeyUtils._
import org.constellation.primitives.Schema._
import org.constellation.schema.Id
import org.constellation.serializer.KryoSerializer
import org.constellation.util.{POWExt, SignHelpExt}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success, Try}

package object constellation extends POWExt with SignHelpExt {

  final val MinimumTime: Long = 1518898908367L

  implicit class EasyFutureBlock[T](f: Future[T]) {

    def get(t: Int = 240): T = {
      import scala.concurrent.duration._
      Await.result(f, t.seconds)
    }

    def getOpt(t: Int = 240): Option[T] = {
      import scala.concurrent.duration._
      Try { Await.result(f, t.seconds) }.toOption
    }

    def getTry(t: Int = 240): Try[T] = {
      import scala.concurrent.duration._
      Try { Await.result(f, t.seconds) }
    }
  }

  implicit def addressToSocket(peerAddress: String): InetSocketAddress =
    peerAddress.split(":") match { case Array(ip, port) => new InetSocketAddress(ip, port.toInt) }

  implicit def socketToAddress(peerAddress: InetSocketAddress): String =
    peerAddress.getHostString + ":" + peerAddress.getPort

  implicit class KryoSerExt(anyRef: AnyRef) {
    def kryo: Array[Byte] = KryoSerializer.serializeAnyRef(anyRef)
  }

  implicit class SHA256Ext(s: String) {

    def sha256: String = Hashing.sha256().hashBytes(s.getBytes()).toString
  }

  implicit class SHA256ByteExt(arr: Array[Byte]) {

    def sha256: String = Hashing.sha256().hashBytes(arr).toString

    def sha256Bytes: Array[Byte] = Hashing.sha256().hashBytes(arr).asBytes()
  }

  def intToByte(myInteger: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(myInteger).array

  def byteToInt(byteBarray: Array[Byte]): Int =
    ByteBuffer.wrap(byteBarray).order(ByteOrder.BIG_ENDIAN).getInt

  def getCCParams(cc: Product) = {
    val values = cc.productIterator
    cc.getClass.getDeclaredFields.map(_.getName -> values.next).toList
  }

  def pprintInet(inetSocketAddress: InetSocketAddress): String =
    s"address: ${inetSocketAddress.getAddress}, hostname: ${inetSocketAddress.getAddress.getHostAddress}, " +
      s"hostString: ${inetSocketAddress.getHostString}, port: ${inetSocketAddress.getPort}"

  implicit def pubKeyToAddress(key: PublicKey): AddressMetaData =
    AddressMetaData(publicKeyToAddressString(key))

  implicit class KeyPairFix(kp: KeyPair) {

    // This is because the equals method on keypair relies on an object hash code instead of an actual check on the data.
    // The equals method for public and private keys is totally fine though.

    def dataEqual(other: KeyPair): Boolean =
      kp.getPrivate == other.getPrivate &&
        kp.getPublic == other.getPublic

    def address: String = publicKeyToAddressString(kp.getPublic)

    def toId: Id = kp.getPublic.toId

  }

  def hashSerialized(obj: AnyRef) = KryoSerializer.serializeAnyRef(obj).sha256
  def hashSerializedBytes(obj: AnyRef) = KryoSerializer.serializeAnyRef(obj).sha256Bytes

  def signHashWithKey(hash: String, privateKey: PrivateKey): String =
    bytes2hex(signData(hash.getBytes())(privateKey))

  def withMetric[F[_]: Sync, A](fa: F[A], prefix: String)(implicit dao: DAO): F[A] =
    fa.flatTap(_ => dao.metrics.incrementMetricAsync[F](s"${prefix}_success")).handleErrorWith { err =>
      dao.metrics.incrementMetricAsync[F](s"${prefix}_failure") >> err.raiseError[F, A]
    }

  def tryWithMetric[T](t: => T, metricPrefix: String)(implicit dao: DAO): Try[T] = {
    val attempt = Try {
      t
    }
    attempt match {
      case Success(x) =>
        dao.metrics.incrementMetric(metricPrefix + "_success")
      case Failure(e) =>
        metricPrefix + ": " + e.printStackTrace()
        dao.metrics.incrementMetric(metricPrefix + "_failure")
    }
    attempt
  }

  def tryToMetric[T](attempt: Try[T], metricPrefix: String)(implicit dao: DAO): Try[T] = {
    attempt match {
      case Success(x) =>
        dao.metrics.incrementMetric(metricPrefix + "_success")
      case Failure(e) =>
        e.printStackTrace()
        dao.metrics.incrementMetric(metricPrefix + "_failure")
    }
    attempt
  }

  implicit class PublicKeyExt(publicKey: PublicKey) {

    def toId: Id = Id(hex)

    def hex: String = publicKeyToHex(publicKey)
  }

  implicit class BoolToOption(val self: Boolean) extends AnyVal {

    def toOption[A](value: => A): Option[A] =
      if (self) Some(value) else None
  }

}
