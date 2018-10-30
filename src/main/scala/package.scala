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
import org.constellation.crypto.KeyUtilsExt
import org.constellation.primitives.IncrementMetric
import org.constellation.primitives.Schema._
import org.constellation.util.{POWExt, POWSignHelp}
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.{Serialization, parseJsonOpt}
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JObject, JValue, ShortTypeHints}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Random, Success, Try}

package object constellation extends KeyUtilsExt with POWExt
  with POWSignHelp {

  val minimumTime : Long = 1518898908367L

  implicit class EasyFutureBlock[T](f: Future[T]) {
    def get(t: Int = 30): T = {
      import scala.concurrent.duration._
      Await.result(f, t.seconds)
    }
    def getOpt(t: Int = 30): Option[T] = {
      import scala.concurrent.duration._
      Try{Await.result(f, t.seconds)}.toOption
    }
  }

  implicit def addressToSocket(peerAddress: String): InetSocketAddress =
    peerAddress.split(":") match { case Array(ip, port) => new InetSocketAddress(ip, port.toInt)}

  implicit def socketToAddress(peerAddress: InetSocketAddress): String =
    peerAddress.getHostString + ":" + peerAddress.getPort

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

  def caseClassToJson(message: Any): String = {
    compactRender(Extraction.decompose(message))
  }

  def parse4s(msg: String) : JValue = parseJsonOpt(msg).get

  def compactRender(msg: JValue): String = Serialization.write(msg)

  implicit class SerExt(jsonSerializable: Any) {
    def json: String = caseClassToJson(jsonSerializable)
    def prettyJson: String = Serialization.writePretty(Extraction.decompose(jsonSerializable))
    def tryJson: Try[String] = Try{caseClassToJson(jsonSerializable)}
    def j: String = json
    def jsonSave(f: String): Unit = File(f).writeText(json)
  }

  implicit class ParseExt(input: String) {
    def jValue: JValue = parse4s(input)
    def jv: JValue = jValue
    def x[T](implicit m: Manifest[T]): T = jv.extract[T](constellationFormats, m)
  }

  implicit class SHA256Ext(s: String) {
    def sha256: String = Hashing.sha256().hashBytes(s.getBytes()).toString
  }


  def intToByte(myInteger: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(myInteger).array

  def byteToInt(byteBarray: Array[Byte]): Int =
    ByteBuffer.wrap(byteBarray).order(ByteOrder.BIG_ENDIAN).getInt


  implicit class HTTPHelp(httpResponse: HttpResponse)
                         (implicit val materialize: ActorMaterializer) {
    def unmarshal: Future[String] = Unmarshal(httpResponse.entity).to[String]
  }

  def pprintInet(inetSocketAddress: InetSocketAddress): String = {
    s"address: ${inetSocketAddress.getAddress}, hostname: ${inetSocketAddress.getHostName}, " +
      s"hostString: ${inetSocketAddress.getHostString}, port: ${inetSocketAddress.getPort}"
  }

  implicit def pubKeyToAddress(key: PublicKey): AddressMetaData =  AddressMetaData(publicKeyToAddressString(key))
  implicit def pubKeysToAddress(key: Seq[PublicKey]): AddressMetaData =  AddressMetaData(publicKeysToAddressString(key))

  implicit class KeyPairFix(kp: KeyPair) {

    // This is because the equals method on keypair relies on an object hash code instead of an actual check on the data.
    // The equals method for public and private keys is totally fine though.

    def dataEqual(other: KeyPair): Boolean = {
      kp.getPrivate == other.getPrivate &&
        kp.getPublic == other.getPublic
    }

    def address: AddressMetaData = pubKeyToAddress(kp.getPublic)

  }


  implicit class ActorQuery(a: ActorRef) {
    import akka.pattern.ask
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    def query[T: ClassTag](m: Any): T = (a ? m).mapTo[T].get()
  }

  def signHashWithKeyB64(hash: String, privateKey: PrivateKey): String = base64(signData(hash.getBytes())(privateKey))

  def tryWithMetric[T](t : => T, metricPrefix: String)(implicit dao: DAO): Try[T] = {
    val attempt = Try{
      t
    }
    attempt match {
      case Success(x) =>
        dao.metricsManager ! IncrementMetric(metricPrefix + "_success")
      case Failure(e) =>
        e.printStackTrace()
        dao.metricsManager ! IncrementMetric(metricPrefix + "_failure")
    }
    attempt
  }

  def tryToMetric[T](attempt : Try[T], metricPrefix: String)(implicit dao: DAO): Try[T] = {
    attempt match {
      case Success(x) =>
        dao.metricsManager ! IncrementMetric(metricPrefix + "_success")
      case Failure(e) =>
        e.printStackTrace()
        dao.metricsManager ! IncrementMetric(metricPrefix + "_failure")
    }
    attempt
  }

  def attemptWithRetry(t : => Boolean, maxRetries: Int = 10, delay: Long = 2000): Boolean = {

    var retries = 0
    var done = false

    do {
      retries += 1
      done = t
      Thread.sleep(delay)
    } while (!done && retries < maxRetries)
    done
  }


  def withTimeout[T](fut:Future[T])(implicit ec:ExecutionContext, after:Duration): Future[T] = {
    val prom = Promise[T]()
    val timeout = TimeoutScheduler.scheduleTimeout(prom, after)
    val combinedFut = Future.firstCompletedOf(List(fut, prom.future))
    fut onComplete{case result => timeout.cancel()}
    combinedFut
  }

  import scala.concurrent.duration._
  def withTimeoutSecondsAndMetric[T](fut:Future[T], metricPrefix: String, timeoutSeconds: Int = 10)
                                    (implicit ec:ExecutionContext, dao: DAO): Future[T] = {
    val prom = Promise[T]()
    val after = timeoutSeconds.seconds
    val timeout = TimeoutScheduler.scheduleTimeout(prom, after)
    val combinedFut = Future.firstCompletedOf(List(fut, prom.future))
    fut onComplete{ _ =>
      timeout.cancel()
    }
    prom.future.onComplete{
      result =>
        if (result.isSuccess) {
          dao.metricsManager ! IncrementMetric(metricPrefix + s"_timeoutAfter${timeoutSeconds}seconds")
        }
        if (result.isFailure) {
          dao.metricsManager ! IncrementMetric(metricPrefix + s"_timeoutFAILUREDEBUGAfter${timeoutSeconds}seconds")
        }
    }

    combinedFut
  }

  def futureTryWithTimeoutMetric[T](t: => T, metricPrefix: String, timeoutSeconds: Int = 10)
                                   (implicit ec:ExecutionContext, dao: DAO): Future[Try[T]] = {
    withTimeoutSecondsAndMetric(
      Future{
        Thread.currentThread().setName(metricPrefix + Random.nextInt(10000))
        tryWithMetric(t, metricPrefix) // This is an inner try as opposed to an onComplete so we can
        // Have different metrics for timeout vs actual failure.
      }(ec),
      metricPrefix,
      timeoutSeconds = timeoutSeconds
    )
  }

}

object TimeoutScheduler{

  import org.jboss.netty.util.{HashedWheelTimer, TimerTask, Timeout}
  import java.util.concurrent.TimeUnit
  import scala.concurrent.duration.Duration
  import scala.concurrent.Promise
  import java.util.concurrent.TimeoutException

  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
  def scheduleTimeout(promise:Promise[_], after:Duration) = {
    timer.newTimeout(new TimerTask{
      def run(timeout:Timeout){
        promise.failure(new TimeoutException("Operation timed out after " + after.toMillis + " millis"))
      }
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }
}