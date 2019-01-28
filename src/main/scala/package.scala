import java.net.InetSocketAddress
import java.nio.{ByteBuffer, ByteOrder}
import java.security.{KeyPair, PrivateKey, PublicKey}
import java.util.concurrent.TimeUnit
import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.Timeout
import better.files.File
import com.google.common.hash.Hashing

import org.constellation.DAO
import org.constellation.crypto.Base58
import org.constellation.crypto.KeyUtils.{bytesToPrivateKey, bytesToPublicKey, _}
import org.constellation.primitives.IncrementMetric
import org.constellation.primitives.Schema._
import org.constellation.util.{EncodedPublicKey, POWExt, POWSignHelp}

import org.json4s.JsonAST.{JInt, JString}
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.{Serialization, parseJsonOpt}
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue, ShortTypeHints}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

package object constellation extends POWExt
  with POWSignHelp {

  val minimumTime: Long = 1518898908367L

  // doc
  implicit class EasyFutureBlock[T](f: Future[T]) {

    // doc
    def get(t: Int = 30): T = {
      import scala.concurrent.duration._
      Await.result(f, t.seconds)
    }

    // doc
    def getOpt(t: Int = 30): Option[T] = {
      import scala.concurrent.duration._
      Try {
        Await.result(f, t.seconds)
      }.toOption
    }
  }

  // doc
  implicit def addressToSocket(peerAddress: String): InetSocketAddress =
    peerAddress.split(":") match {
      case Array(ip, port) => new InetSocketAddress(ip, port.toInt)
    }

  // doc
  implicit def socketToAddress(peerAddress: InetSocketAddress): String =
    peerAddress.getHostString + ":" + peerAddress.getPort

  // doc
  class InetSocketAddressSerializer extends CustomSerializer[InetSocketAddress](format => ( {
    case jstr: JObject =>
      val host = (jstr \ "host").extract[String]
      val port = (jstr \ "port").extract[Int]
      new InetSocketAddress(host, port)
  }, {
    case key: InetSocketAddress =>
      JObject("host" -> JString(key.getHostString), "port" -> JInt(key.getPort))
  }
  ))

  implicit val constellationFormats: Formats = DefaultFormats +
    new PublicKeySerializer + new PrivateKeySerializer + new KeyPairSerializer + new InetSocketAddressSerializer +
    ShortTypeHints(List(classOf[TransactionHash], classOf[ParentBundleHash])) + new EnumNameSerializer(EdgeHashType) +
    new EnumNameSerializer(NodeState)

  // doc
  def caseClassToJson(message: Any): String = {
    compactRender(Extraction.decompose(message))
  }

  // doc
  def parse4s(msg: String): JValue = parseJsonOpt(msg).get

  // doc
  def compactRender(msg: JValue): String = Serialization.write(msg)

  // doc
  implicit class SerExt(jsonSerializable: Any) {

    // doc
    def json: String = caseClassToJson(jsonSerializable)

    // doc
    def prettyJson: String = Serialization.writePretty(Extraction.decompose(jsonSerializable))

    // doc
    def tryJson: Try[String] = Try {
      caseClassToJson(jsonSerializable)
    }

    // doc
    def j: String = json

    // doc
    def jsonSave(f: String): Unit = File(f).writeText(json)
  }

  // doc
  implicit class ParseExt(input: String) {

    // doc
    def jValue: JValue = parse4s(input)

    // doc
    def jv: JValue = jValue

    // doc
    def x[T](implicit m: Manifest[T]): T = jv.extract[T](constellationFormats, m)
  }

  // doc
  implicit class SHA256Ext(s: String) {

    // doc
    def sha256: String = Hashing.sha256().hashBytes(s.getBytes()).toString
  }

  // doc
  def intToByte(myInteger: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(myInteger).array

  // doc
  def byteToInt(byteBarray: Array[Byte]): Int =
    ByteBuffer.wrap(byteBarray).order(ByteOrder.BIG_ENDIAN).getInt

  // doc
  implicit class HTTPHelp(httpResponse: HttpResponse)
                         (implicit val materialize: ActorMaterializer) {

    // doc
    def unmarshal: Future[String] = Unmarshal(httpResponse.entity).to[String]
  }

  // doc
  def pprintInet(inetSocketAddress: InetSocketAddress): String = {
    s"address: ${inetSocketAddress.getAddress}, hostname: ${inetSocketAddress.getHostName}, " +
      s"hostString: ${inetSocketAddress.getHostString}, port: ${inetSocketAddress.getPort}"
  }

  // doc
  implicit def pubKeyToAddress(key: PublicKey): AddressMetaData = AddressMetaData(publicKeyToAddressString(key))

  // doc
  implicit def pubKeysToAddress(key: Seq[PublicKey]): AddressMetaData = AddressMetaData(publicKeysToAddressString(key))

  // doc
  implicit class KeyPairFix(kp: KeyPair) {

    // This is because the equals method on keypair relies on an object hash code instead of an actual check on the data.
    // The equals method for public and private keys is totally fine though.

    // doc
    def dataEqual(other: KeyPair): Boolean = {
      kp.getPrivate == other.getPrivate &&
        kp.getPublic == other.getPublic
    }

    // doc
    def address: AddressMetaData = pubKeyToAddress(kp.getPublic)

  }

  // doc
  implicit class ActorQuery(a: ActorRef) {

    import akka.pattern.ask

    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)

    // doc
    def query[T: ClassTag](m: Any): T = (a ? m).mapTo[T].get()
  }

  // doc
  def signHashWithKeyB64(hash: String, privateKey: PrivateKey): String = base64(signData(hash.getBytes())(privateKey))

  // doc
  def tryWithMetric[T](t: => T, metricPrefix: String)(implicit dao: DAO): Try[T] = {
    val attempt = Try {
      t
    }
    attempt match {
      case Success(x) =>
        dao.metricsManager ! IncrementMetric(metricPrefix + "_success")
      case Failure(e) =>
        metricPrefix + ": " + e.printStackTrace()
        dao.metricsManager ! IncrementMetric(metricPrefix + "_failure")
    }
    attempt
  }

  // doc
  def tryToMetric[T](attempt: Try[T], metricPrefix: String)(implicit dao: DAO): Try[T] = {
    attempt match {
      case Success(x) =>
        dao.metricsManager ! IncrementMetric(metricPrefix + "_success")
      case Failure(e) =>
        e.printStackTrace()
        dao.metricsManager ! IncrementMetric(metricPrefix + "_failure")
    }
    attempt
  }

  // doc
  def attemptWithRetry(t: => Boolean, maxRetries: Int = 10, delay: Long = 2000): Boolean = {

    var retries = 0
    var done = false

    do {
      retries += 1
      done = t
      val normalizedDelay = delay + Random.nextInt((delay * (scala.math.pow(2, retries))).toInt)
      Thread.sleep(normalizedDelay)
    } while (!done && retries < maxRetries)
    done
  }

  // doc
  def withTimeout[T](fut: Future[T])(implicit ec: ExecutionContext, after: Duration): Future[T] = {
    val prom = Promise[T]()
    val timeout = TimeoutScheduler.scheduleTimeout(prom, after)
    val combinedFut = Future.firstCompletedOf(List(fut, prom.future))
    fut onComplete { case result => timeout.cancel() }
    combinedFut
  }

  import scala.concurrent.duration._

  // doc
  def withTimeoutSecondsAndMetric[T](fut: Future[T], metricPrefix: String, timeoutSeconds: Int = 10, onError: => Unit = ())
                                    (implicit ec: ExecutionContext, dao: DAO): Future[T] = {
    val prom = Promise[T]()
    val after = timeoutSeconds.seconds
    val timeout = TimeoutScheduler.scheduleTimeout(prom, after)
    val combinedFut = Future.firstCompletedOf(List(fut, prom.future))
    fut onComplete { _ =>
      timeout.cancel()
    }
    prom.future.onComplete {
      result =>
        onError
        if (result.isSuccess) {
          dao.metricsManager ! IncrementMetric(metricPrefix + s"_timeoutAfter${timeoutSeconds}seconds")
        }
        if (result.isFailure) {
          dao.metricsManager ! IncrementMetric(metricPrefix + s"_timeoutFAILUREDEBUGAfter${timeoutSeconds}seconds")
        }
    }

    combinedFut
  }

  // doc
  def futureTryWithTimeoutMetric[T](t: => T, metricPrefix: String, timeoutSeconds: Int = 10, onError: => Unit = ())
                                   (implicit ec: ExecutionContext, dao: DAO): Future[Try[T]] = {
    withTimeoutSecondsAndMetric(
      Future {
        val originalName = Thread.currentThread().getName
        Thread.currentThread().setName(metricPrefix + Random.nextInt(10000))
        val attempt = tryWithMetric(t, metricPrefix) // This is an inner try as opposed to an onComplete so we can
        // Have different metrics for timeout vs actual failure.
        Thread.currentThread().setName(originalName)
        attempt
      }(ec),
      metricPrefix,
      timeoutSeconds = timeoutSeconds
    )
  }

  // doc
  def withRetries(
                   err: String,
                   t: => Boolean,
                   maxRetries: Int = 10,
                   delay: Long = 10000
                 ): Boolean = {

    var retries = 0
    var done = false

    do {
      retries += 1
      done = t
      Thread.sleep(delay)
    } while (!done && retries < maxRetries)
    done
  }

  // doc
  class PrivateKeySerializer extends CustomSerializer[PrivateKey](format => ( {
    case jObj: JObject =>
      // implicit val f: Formats = format
      bytesToPrivateKey(Base58.decode((jObj \ "key").extract[String]))
  }, {
    case key: PrivateKey =>
      JObject("key" -> JString(Base58.encode(key.getEncoded)))
  }
  ))

  // doc
  class PublicKeySerializer extends CustomSerializer[PublicKey](format => ( {
    case jstr: JObject =>
      // implicit val f: Formats = format
      bytesToPublicKey(Base58.decode((jstr \ "key").extract[String]))
  }, {
    case key: PublicKey =>
      JObject("key" -> JString(Base58.encode(key.getEncoded)))
  }
  ))

  // doc
  class KeyPairSerializer extends CustomSerializer[KeyPair](format => ( {
    case jObj: JObject =>
      //  implicit val f: Formats = format
      val pubKey = (jObj \ "publicKey").extract[PublicKey]
      val privKey = (jObj \ "privateKey").extract[PrivateKey]
      val kp = new KeyPair(pubKey, privKey)
      kp
  }, {
    case key: KeyPair =>
      //  implicit val f: Formats = format
      JObject(
        "publicKey" -> JObject("key" -> JString(Base58.encode(key.getPublic.getEncoded))),
        "privateKey" -> JObject("key" -> JString(Base58.encode(key.getPrivate.getEncoded)))
      )
  }
  ))

  // doc
  implicit class PublicKeyExt(publicKey: PublicKey) {
    // Conflict with old schema, add later
    //  def address: Address = pubKeyToAddress(publicKey)

    // doc
    def encoded: EncodedPublicKey = EncodedPublicKey(Base58.encode(publicKey.getEncoded))

    // doc
    def toId: Id = encoded.toId
  }

}

// doc
object TimeoutScheduler {

  import java.util.concurrent.{TimeUnit, TimeoutException}

  import org.jboss.netty.util.{HashedWheelTimer, Timeout, TimerTask}

  import scala.concurrent.Promise
  import scala.concurrent.duration.Duration

  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)

  // doc
  def scheduleTimeout(promise: Promise[_], after: Duration) = {
    timer.newTimeout(new TimerTask {

      // doc
      def run(timeout: Timeout) {
        promise.failure(new TimeoutException("Operation timed out after " + after.toMillis + " millis"))
      }
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }
}
