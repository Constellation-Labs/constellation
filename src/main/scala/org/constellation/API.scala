package org.constellation

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives.{entity, path, _}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.Credentials
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.softwaremill.macmemo.memoize
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.LevelDB.DBPut
import org.constellation.crypto.Wallet
import org.constellation.primitives.Schema._
import org.constellation.primitives.{APIBroadcast, Schema, UpdateMetric}
import org.constellation.util.{ServeUI, Metrics}
import org.json4s.native
import org.json4s.native.Serialization
import scalaj.http.HttpResponse

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

case class AddPeerRequest(host: String, udpPort: Int, httpPort: Int, id: Id)

class API(
           val peerToPeerActor: ActorRef,
           consensusActor: ActorRef,
           udpAddress: InetSocketAddress,
           val data: Data = null,
           peerManager: ActorRef,
           metricsManager: ActorRef,
           cellManager: ActorRef
         )(implicit executionContext: ExecutionContext, val timeout: Timeout)
  extends Json4sSupport
    with Wallet
    with ServeUI {

  import data._

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  val logger = Logger(s"APIInterface")

  val config: Config = ConfigFactory.load()

  val authId = config.getString("auth.id")
  val authPassword = config.getString("auth.password")

  val getEndpoints: Route =
    extractClientIP { clientIP =>
      get {
        path("balance") {
          complete(validLedger.getOrElse(selfAddress.address, 0L).toString)
        } ~
          path("bundles") {
            complete(last100ValidBundleMetaData)
          } ~
          path("restart") {
            data.restartNode()
            complete(StatusCodes.OK)
          } ~
          path("submitTX") {
            parameter('address, 'amount) { (address, amount) =>
              logger.error(s"SubmitTX : $address $amount")
              handleSendRequest(SendToAddress(address, amount.toLong))
            }
          } ~
          pathPrefix("address" / Remaining) { hash =>
            val balance = validLedger.getOrElse(hash, 0).toString

            complete(s"Balance: $balance")
          } ~
          pathPrefix("txHash") { // TODO: remove reference in ui and use the /transaction route instead
            extractUnmatchedPath { p =>
              val transactionHash = p.toString().tail

              complete(lookupTransactionDB(transactionHash).prettyJson)
            }
          } ~
          pathPrefix("transaction") {
            extractUnmatchedPath { p =>
              val transactionHash = p.toString().tail

              complete(lookupTransactionDB(transactionHash).prettyJson)
            }
          } ~
          pathPrefix("maxBundle") {
            val genesisTx = genesisBundle.map(b => b.extractTXDB.head)

            if (genesisTx.isDefined && maxBundle.isDefined) {
              val maybeSheaf = lookupBundleDBFallbackBlocking(maxBundle.get.hash)

              complete(Some(MaxBundleGenesisHashQueryResponse(genesisBundle, genesisTx, maybeSheaf)))
            } else {
              complete(None)
            }
          } ~
          pathPrefix("bundle") {
            extractUnmatchedPath { p =>
              logger.debug(s"Unmatched path on bundle direct result $p")
              val bundleHash = p.toString().tail

              val maybeSheaf = lookupBundleDBFallbackBlocking(bundleHash)

              complete(maybeSheaf)
            }
          } ~
          pathPrefix("fullBundle") {
            extractUnmatchedPath { p =>
              logger.debug(s"Unmatched path on fullBundle result $p")
              val bundleHash = p.toString().split("/").last
              val maybeSheaf = lookupBundleDBFallbackBlocking(bundleHash)
              complete(
                BundleHashQueryResponse(
                  bundleHash,
                  maybeSheaf,
                  maybeSheaf
                    .map(_.bundle.extractTXDB.toSeq.sortBy {
                      _.txData.time
                    })
                    .getOrElse(Seq())
                )
              )
            }
          } ~ pathPrefix("download") {
          extractUnmatchedPath { p =>
            Try {
              val bundleHash = p.toString().split("/").last

              val ancestors = findAncestorsUpTo(bundleHash, Seq(), upTo = 10)

              val res = ancestors.map { a =>
                BundleHashQueryResponse(
                  a.bundle.hash,
                  Some(a),
                  a.bundle.extractTXDB.toSeq.sortBy {
                    _.txData.time
                  }
                )
              }

              complete(res)
            } match {
              case Success(x) => x
              case Failure(e) =>
                e.printStackTrace()
                complete(StatusCodes.InternalServerError)
            }
          }
        } ~
          pathPrefix("ancestors") {
            extractUnmatchedPath { p =>
              logger.debug(s"Unmatched path on download result $p")
              val ps = p.toString().split("/").last

              val ancestors = findAncestorsUpTo(ps, Seq(), upTo = 101)

              complete(ancestors.map {
                _.bundle.hash
              })
            }
          } ~
          path("setKeyPair") {
            parameter('keyPair) { kpp =>
              logger.debug("Set key pair " + kpp)
              val res = if (kpp.length > 10) {
                val rr = Try {
                  data.updateKeyPair(kpp.x[KeyPair])
                  StatusCodes.OK
                }.getOrElse(StatusCodes.BadRequest)
                rr
              } else StatusCodes.BadRequest
              complete(res)
            }
          } ~
          path("metrics") {
            complete(new util.Metrics(data).calculateMetrics())
          } ~
          path("validTX") {
            complete(last10000ValidTXHash)
          } ~
          path("makeKeyPair") {
            val pair = constellation.makeKeyPair()
            wallet :+= pair
            complete(pair)
          } ~
          path("genesis" / LongNumber) { numCoins =>
            val ret = if (genesisBundle.isEmpty) {
              val debtAddress = walletPair
              val tx = createTransaction(selfAddress.address,
                numCoins,
                src = debtAddress.address.address)
              createGenesis(tx)
              tx
            } else genesisBundle.get.extractTX.head
            complete(ret)
          } ~
          path("stackSize" / IntNumber) { num =>
            minGenesisDistrSize = num
            complete(StatusCodes.OK)
          } ~
          path("makeKeyPairs" / IntNumber) { numPairs =>
            val pair = Seq.fill(numPairs) {
              constellation.makeKeyPair()
            }
            wallet ++= pair
            complete(pair)
          } ~
          path("wallet") {
            complete(wallet)
          } ~
          path("selfAddress") {
            complete(id.address)
          } ~
          path("id") {
            complete(id)
          } ~
          path("nodeKeyPair") {
            complete(keyPair)
          } ~
          // TODO: revisit
          path("health") {
            complete(StatusCodes.OK)
          } ~
          path("peers") {
            complete(Peers(peerIPs.toSeq))
          } ~
          path("peerids") {
            complete(peers.map {
              _.data
            })
          } ~
          path("actorPath") {
            complete(peerToPeerActor.path.toSerializationFormat)
          } ~
          path("dashboard") {

            val transactions = last100ValidBundleMetaData.reverse
              .take(20)
              .map {
                _.bundle
              }
              .map(b => {
                (b.extractTXDB.toSeq.sortBy(_.txData.time),
                  b.extractIds.map(f => f.address.address))
              })
              .flatMap(t => {
                t._1.map(
                  e =>
                    TransactionSerialized(e.hash,
                      e.txData.data.src,
                      e.txData.data.dst,
                      e.txData.data.normalizedAmount,
                      t._2,
                      e.txData.time
                    ))
              })

            var peerMap: Seq[Node] = peers
              .map {
                _.data
              }
              .seq
              .map { f =>
              {
                Node(f.id.address.address,
                  f.externalAddress
                    .map {
                      _.getHostName
                    }
                    .getOrElse(""),
                  f.externalAddress
                    .map {
                      _.getPort
                    }
                    .getOrElse(0))
              }
              }

            // Add self
            peerMap = peerMap :+ Node(selfAddress.address,
              selfPeer.data.externalAddress
                .map {
                  _.getHostName
                }
                .getOrElse(""),
              selfPeer.data.externalAddress
                .map {
                  _.getPort
                }
                .getOrElse(0))

            complete(
              Map(
                "peers" -> peerMap,
                "transactions" -> transactions
              ))

          } ~
          jsRequest ~
          serveMainPage
      }
    }

  private val postEndpoints =
    post {
      path("startRandomTX") {
        sendRandomTXV2 = !sendRandomTXV2
        complete(sendRandomTXV2.toString)
      } ~
      path("peerHealthCheckV2") {
        val response = (peerManager ? APIBroadcast(_.get("health"))).mapTo[Map[Id, Future[HttpResponse[String]]]]
        val res = response.getOpt().map{
          idMap =>

            val res = idMap.map{
              case (id, fut) =>
                val maybeResponse = fut.getOpt()
             //   println(s"Maybe response $maybeResponse")
                id -> maybeResponse.exists{_.isSuccess}
            }.toSeq

            complete(res)

        }.getOrElse(complete(StatusCodes.InternalServerError))

        res
      } ~
        pathPrefix("genesis") {
          path("create") {
            entity(as[Set[Id]]) { ids =>
              complete(createGenesisAndInitialDistributionOE(ids))
            }
          } ~
          path("accept") {
            entity(as[GenesisObservation]) { go =>
              acceptGenesisOE(go)
              // TODO: Report errors and add validity check
              complete(StatusCodes.OK)
            }
          }
        } ~
        path("initializeDownload") {
          downloadMode = true
          complete(StatusCodes.OK)
        } ~
        path("disableDownload") {
          downloadMode = false
          downloadInProgress = false
          complete(StatusCodes.OK)
        } ~
        path("completeUpload") {
          entity(as[BundleHashQueryResponse]) { b =>
            val s = b.sheaf.get
            maxBundleMetaData = Some(s)
            downloadInProgress = false
            downloadMode = false
            complete(StatusCodes.OK)
          }
        } ~
        path("upload") {
          entity(as[Seq[BundleHashQueryResponse]]) {
            bhqr =>
              bhqr.foreach{ b =>
                val s = b.sheaf.get
                if (s.height.get == 0) {
                  acceptGenesis(s.bundle, b.transactions.head)
                } else {
                  putBundleDB(s)
                  totalNumValidBundles += 1
                  b.transactions.foreach{t =>
                    putTXDB(t)
                    b.transactions.foreach{acceptTransaction}
                    t.txData.data.updateLedger(memPoolLedger)
                  }
                }
              }
              complete(StatusCodes.OK)
          }
        } ~
        path("rxBundle") {
          entity(as[Bundle]) { bundle =>
           peerToPeerActor ! bundle

           complete(StatusCodes.OK)
          }
        } ~
        path("handleTransaction") {
          entity(as[TransactionV1]) { tx =>
            peerToPeerActor ! TransactionV1

            complete(StatusCodes.OK)
          }
        } ~
        path("batchBundleRequest") {
          entity(as[BatchBundleHashRequest]) { bhr =>
            val sheafResponses = bhr.hashes.flatMap {
              lookupBundleDBFallbackBlocking
            }
            complete(sheafResponses)
          }
        } ~
        path("batchTXRequest") {
          entity(as[BatchTXHashRequest]) { bhr =>
            complete(bhr.hashes.flatMap{lookupTransactionDBFallbackBlocking})
          }
        } ~
        path("peerSyncHeartbeat") {
          entity(as[PeerSyncHeartbeat]) { psh =>
            processPeerSyncHeartbeat(psh)
            complete(StatusCodes.OK)
          }
        } ~
        path("sendToAddress") {
          entity(as[SendToAddress]) { s =>
            handleSendRequest(s)
          }
        } ~
        path("tx") {
          entity(as[TransactionV1]) { tx =>
            peerToPeerActor ! tx
            complete(StatusCodes.OK)
          }
        } ~
        path("addPeerV2"){
          entity(as[AddPeerRequest]) { e =>

            peerManager ! e

            complete(StatusCodes.OK)
          }
        } ~
        path("peer") {
          entity(as[String]) { peerAddress =>

            Try {
              //    logger.debug(s"Received request to add a new peer $peerAddress")
              var addr: Option[InetSocketAddress] = None
              val result = Try {
                peerAddress.replaceAll('"'.toString, "").split(":") match {
                  case Array(ip, port) => new InetSocketAddress(ip, port.toInt)
                  case a@_ => logger.debug(s"Unmatched Array: $a"); throw new RuntimeException(s"Bad Match: $a");
                }
              }.toOption match {
                case None =>
                  StatusCodes.BadRequest
                case Some(v) =>
                  addr = Some(v)
                  val fut = (peerToPeerActor ? AddPeerFromLocal(v)).mapTo[StatusCode]
                  val res = Try {
                    Await.result(fut, timeout.duration)
                  }.toOption
                  res match {
                    case None =>
                      StatusCodes.RequestTimeout
                    case Some(f) =>
                      if (f == StatusCodes.Accepted) {
                        var attempts = 0
                        var peerAdded = false
                        while (attempts < 5) {
                          attempts += 1
                          Thread.sleep(1500)
                          //peerAdded = peers.exists(p => v == p.data.externalAddress)
                          peerAdded = signedPeerLookup.contains(v)
                        }
                        if (peerAdded) StatusCodes.OK else StatusCodes.NetworkConnectTimeout
                      } else f
                  }
              }

              if (result != StatusCodes.OK) {
                addr.foreach(peersAwaitingAuthenticationToNumAttempts(_) = 1)
              }

              logger.debug(s"New peer request $peerAddress statusCode: $result")
              result
            } match {
              case Failure(e) =>
                e.printStackTrace()
                complete(StatusCodes.InternalServerError)
              case Success(x) => complete(x)
            }
          }
        } ~
        path("ip") {
          entity(as[String]) { externalIp =>
            var ipp: String = ""
            val addr =
              externalIp.replaceAll('"'.toString, "").split(":") match {
                case Array(ip, port) =>
                  ipp = ip
                  externalHostString = ip
                  new InetSocketAddress(ip, port.toInt)
                case a @ _ => {
                  logger.debug(s"Unmatched Array: $a")
                  throw new RuntimeException(s"Bad Match: $a")
                }
              }
            logger.debug(s"Set external IP RPC request $externalIp $addr")
            data.externalAddress = Some(addr)
            if (ipp.nonEmpty)
              data.apiAddress = Some(new InetSocketAddress(ipp, 9000))
            complete(StatusCodes.OK)
          }
        } ~
        path("reputation") {
          entity(as[Seq[UpdateReputation]]) { ur =>
            secretReputation = ur.flatMap { r =>
              r.secretReputation.map {
                id -> _
              }
            }.toMap
            publicReputation = ur.flatMap { r =>
              r.publicReputation.map {
                id -> _
              }
            }.toMap
            complete(StatusCodes.OK)
          }
        }
    }

  private val faviconRoute = get {
    path("favicon.ico") {
      getFromResource("favicon.ico")
    }
  }

  private val routes: Route = cors() {
    getEndpoints ~ postEndpoints ~ jsRequest ~ serveMainPage
  }

  def myUserPassAuthenticator(credentials: Credentials): Option[String] = {
    credentials match {
      case p @ Credentials.Provided(id)
        if id == authId && p.verify(authPassword) =>
        Some(id)
      case _ => None
    }
  }

  val authRoutes = faviconRoute ~ routes

}
