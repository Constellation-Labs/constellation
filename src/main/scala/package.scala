import java.net.InetSocketAddress
import java.nio.{ByteBuffer, ByteOrder}
import java.security.{KeyPair, PrivateKey, PublicKey}
import java.util.concurrent.TimeUnit

import cats.effect.Sync
import cats.syntax.all._
import com.google.common.hash.Hashing
import org.constellation.DAO
import org.constellation.keytool.KeyUtils._
import org.constellation.schema.v2.Schema._
import org.constellation.schema.v2.Id
import org.constellation.schema.v2.signature.SignHelpExt
import org.constellation.serializer.KryoSerializer
import org.constellation.util.POWExt

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success, Try}

package object constellation extends POWExt with SignHelpExt {

  implicit def addressToSocket(peerAddress: String): InetSocketAddress =
    peerAddress.split(":") match { case Array(ip, port) => new InetSocketAddress(ip, port.toInt) }

  implicit def socketToAddress(peerAddress: InetSocketAddress): String =
    peerAddress.getHostString + ":" + peerAddress.getPort

  implicit class SHA256Ext(s: String) {

    def sha256: String = Hashing.sha256().hashBytes(s.getBytes()).toString
  }

  implicit class SHA256ByteExt(arr: Array[Byte]) {

    def sha256: String = Hashing.sha256().hashBytes(arr).toString

    def sha256Bytes: Array[Byte] = Hashing.sha256().hashBytes(arr).asBytes()
  }

  def withMetric[F[_]: Sync, A](fa: F[A], prefix: String)(implicit dao: DAO): F[A] =
    fa.flatTap(_ => dao.metrics.incrementMetricAsync[F](s"${prefix}_success")).handleErrorWith { err =>
      dao.metrics.incrementMetricAsync[F](s"${prefix}_failure") >> err.raiseError[F, A]
    }

  implicit class PublicKeyExt(publicKey: PublicKey) {

    def toId: Id = Id(hex)

    def hex: String = publicKeyToHex(publicKey)
  }

}
