package org.constellation.util

import akka.http.scaladsl.coding.Gzip
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

import org.constellation.DAO
import org.constellation.consensus.{Snapshot, SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Schema.{Id, MetricsResult}
import org.constellation.serializer.KryoSerializer

import org.json4s.native.Serialization
import org.json4s.{Formats, native}
import com.softwaremill.sttp._
import com.softwaremill.sttp.json4s._
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import com.softwaremill.sttp.prometheus.PrometheusBackend
import com.typesafe.scalalogging.Logger
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** API client object. */
object APIClient {

  /** @todo: documentation */
  def apply(host: String = "127.0.0.1", port: Int, peerHTTPPort: Int = 9001, internalPeerHost: String = "")
           (
             implicit executionContext: ExecutionContext,
             dao: DAO = null
           ): APIClient = {
    new APIClient(host, port, peerHTTPPort, internalPeerHost)(executionContext, dao)
  }
}

/** API client class. */
class APIClient private(host: String = "127.0.0.1", port: Int, val peerHTTPPort: Int = 9001, val internalPeerHost: String = "")(
  // implicit val system: ActorSystem, // tmp comment
  implicit val executionContext: ExecutionContext,
  dao: DAO = null
) {

  implicit val backend = new LoggingSttpBackend[Future, Nothing](PrometheusBackend[Future, Nothing](OkHttpFutureBackend()))

  implicit val serialization = native.Serialization

  val logger = Logger(s"APIClient(host=$host, port=$port)")

  val daoOpt = Option(dao)

  val hostName: String = host

  var id: Id = _

  val udpPort: Int = 16180

  val apiPort: Int = port

  /** @return UDP address constructed from host class constructor input. */
  def udpAddress: String = hostName + ":" + udpPort

  /** @return Whether ??. */
  def setExternalIP(): Boolean = postSync("ip", hostName + ":" + udpPort).isSuccess

  /** @return Uniform Resource Identifier string. */
  private def baseURI: String = {
    val uri = s"http://$hostName:$apiPort"
    uri
  }

  /** Sets the password. */
  def setPassword(newPassword: String) = authPassword = newPassword

  /** @return baseURI string ??. */
  def base(suffix: String) = s"$baseURI/$suffix"

  /** ??.
    *
    * @todo Fix almost name clash.
    */
  private def baseUri(suffix: String) = s"$baseURI/$suffix"

  private val config = ConfigFactory.load()

  private val authEnabled = config.getBoolean("auth.enabled")

  private val authId = config.getString("auth.id")

  private var authPassword = config.getString("auth.password")

  // doc
  implicit class AddBlocking[T](req: Future[T]) {

    /** @return ??. */
    def blocking(timeout: Duration = 60.seconds): T = {
      Await.result(req, timeout)
    }

  }

  // doc
  def optHeaders: Map[String, String] = daoOpt.map {
    d =>
      Map("Remote-Address" -> d.externalHostString, "X-Real-IP" -> d.externalHostString)
  }.getOrElse(Map())

  // doc
  def httpWithAuth(suffix: String, params: Map[String, String] = Map.empty, timeout: Duration = 5.seconds)(method: Method) = {
    val base = baseUri(suffix)
    val uri = uri"$base?$params"
    val req = sttp.method(method, uri).readTimeout(timeout).headers(optHeaders)
    if (authEnabled) {
      req.auth.basic(authId, authPassword)
    } else req
  }

  // doc
  def metrics: Map[String, String] = {
    getBlocking[MetricsResult]("metrics", timeout = 5.seconds).metrics
  }

  /** API client post. */
  def post(suffix: String, b: AnyRef, timeout: Duration = 5.seconds)
          (implicit f: Formats = constellation.constellationFormats): Future[Response[String]] = {
    val ser = Serialization.write(b)
    val gzipped = Gzip.encode(ByteString.fromString(ser)).toArray
    httpWithAuth(suffix, timeout = timeout)(Method.POST)
      .body(gzipped)
      .contentType("application/json")
      .header("Content-Encoding", "gzip")
      .send()
  }

  /** API client put. */
  def put(suffix: String, b: AnyRef, timeout: Duration = 5.seconds)
         (implicit f: Formats = constellation.constellationFormats): Future[Response[String]] = {
    val ser = Serialization.write(b)
    val gzipped = Gzip.encode(ByteString.fromString(ser)).toArray
    httpWithAuth(suffix, timeout = timeout)(Method.PUT)
      .body(gzipped)
      .contentType("application/json")
      .header("Content-Encoding", "gzip")
      .send()
  }

  // doc
  def postEmpty(suffix: String, timeout: Duration = 5.seconds)(implicit f: Formats = constellation.constellationFormats)
  : Response[String] = {
    httpWithAuth(suffix, timeout = timeout)(Method.POST).send().blocking()
  }

  // doc
  def postSync(suffix: String, b: AnyRef, timeout: Duration = 5.seconds)(
    implicit f: Formats = constellation.constellationFormats
  ): Response[String] = {
    post(suffix, b, timeout).blocking(timeout)
  }

  // doc
  def putSync(suffix: String, b: AnyRef, timeout: Duration = 5.seconds)(
    implicit f: Formats = constellation.constellationFormats
  ): Response[String] = {
    put(suffix, b, timeout).blocking(timeout)
  }

  // doc
  def postBlocking[T <: AnyRef](suffix: String, b: AnyRef, timeout: Duration = 5.seconds)(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): T = {
    postNonBlocking(suffix, b, timeout).blocking(timeout)
  }

  // doc
  def postNonBlocking[T <: AnyRef](suffix: String, b: AnyRef, timeout: Duration = 5.seconds)(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): Future[T] = {
    val ser = Serialization.write(b)
    val gzipped = Gzip.encode(ByteString.fromString(ser)).toArray
    httpWithAuth(suffix, timeout = timeout)(Method.POST)
      .body(gzipped)
      .contentType("application/json")
      .header("Content-Encoding", "gzip")
      .response(asJson[T])
      .send()
      .map(_.unsafeBody)
  }

  // doc
  def postBlockingEmpty[T <: AnyRef](suffix: String, timeout: Duration = 5.seconds)(implicit m: Manifest[T], f: Formats = constellation.constellationFormats): T = {
    val res = postEmpty(suffix, timeout)
    Serialization.read[T](res.unsafeBody)
  }

  /** API client get. */
  def get(suffix: String, queryParams: Map[String, String] = Map(), timeout: Duration = 5.seconds): Future[Response[String]] = {
    httpWithAuth(suffix, queryParams, timeout)(Method.GET).send()
  }

  // doc
  def getSync(suffix: String, queryParams: Map[String, String] = Map(), timeout: Duration = 5.seconds): Response[String] = {
    get(suffix, queryParams, timeout).blocking(timeout)
  }

  // doc
  def getBlocking[T <: AnyRef](suffix: String, queryParams: Map[String, String] = Map(), timeout: Duration = 5.seconds)
                              (implicit m: Manifest[T], f: Formats = constellation.constellationFormats): T = {
    getNonBlocking[T](suffix, queryParams, timeout).blocking(timeout)
  }

  // doc
  def getNonBlocking[T <: AnyRef](suffix: String, queryParams: Map[String, String] = Map(), timeout: Duration = 5.seconds)
                                 (implicit m: Manifest[T], f: Formats = constellation.constellationFormats): Future[T] = {
    httpWithAuth(suffix, queryParams, timeout)(Method.GET)
      .response(asJson[T])
      .send()
      .map(_.unsafeBody)
  }

  // doc
  def getNonBlockingStr(suffix: String, queryParams: Map[String, String] = Map(), timeout: Duration = 5.seconds): Future[String] = {
    httpWithAuth(suffix, queryParams, timeout)(Method.GET).send().map { x => x.unsafeBody }
  }

  // doc
  def getBlockingBytesKryo[T <: AnyRef](suffix: String, queryParams: Map[String, String] = Map(), timeout: Duration = 5.seconds): T = {
    val resp = httpWithAuth(suffix, queryParams, timeout)(Method.GET).response(asByteArray).send().blocking()
    KryoSerializer.deserializeCast[T](resp.unsafeBody)
  }

  // doc
  def getSnapshotInfo(): SnapshotInfo = getBlocking[SnapshotInfo]("info")

  /** @return Sapshots. */
  def getSnapshots(): Seq[Snapshot] = {

    val snapshotInfo = getSnapshotInfo()

    val startingSnapshot = snapshotInfo.snapshot

    /** getSnapshots helper. */
    def getSnapshots(hash: String, snapshots: Seq[Snapshot] = Seq()): Seq[Snapshot] = {
      val sn = getBlocking[Option[Snapshot]]("snapshot/" + hash)
      sn match {
        case Some(snapshot) =>
          if (snapshot.lastSnapshot == "" || snapshot.lastSnapshot == Snapshot.snapshotZeroHash) {
            snapshots :+ snapshot
          } else {
            getSnapshots(snapshot.lastSnapshot, snapshots :+ snapshot)
          }
        case None =>
          logger.warn("MISSING SNAPSHOT")
          snapshots
      }
    }

    val snapshots = getSnapshots(startingSnapshot.lastSnapshot, Seq(startingSnapshot))

    snapshots
  }

  // doc
  def simpleDownload(): Seq[StoredSnapshot] = {

    val hashes = getBlocking[Seq[String]]("snapshotHashes")

    hashes.map { h =>
      getBlockingBytesKryo[StoredSnapshot]("storedSnapshot/" + h)
    }

  }

} // end class APIClient
