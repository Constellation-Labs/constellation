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
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import constellation._
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.LevelDB.DBPut
import org.constellation.crypto.Wallet
import org.constellation.primitives.{APIBroadcast, MetricsManager, Schema, UpdateMetric}
import org.constellation.primitives.Schema._
import org.constellation.util.{HashSignature, ServeUI}
import org.json4s.native
import org.json4s.native.Serialization

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
           nodeManager: ActorRef,
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

  val config = ConfigFactory.load()

  val authId = config.getString("auth.id")
  val authPassword = config.getString("auth.password")

  @memoize(maxSize = 1, expiresAfter = 2.seconds)
  def calculateMetrics(): Metrics = {
    Metrics(
      Map(
        "version" -> "1.0.2",
        "allPeersHealthy" -> allPeersHealthy.toString,
        //   "numAPICalls" -> numAPICalls.toString,
        "numMempoolEmits" -> numMempoolEmits.toString,
        "numDBGets" -> numDBGets.toString,
        "numDBPuts" -> numDBPuts.toString,
        "numDBDeletes" -> numDBDeletes.toString,
        "numTXRemovedFromMemory" -> numTXRemovedFromMemory.toString,
        "numDeletedBundles" -> numDeletedBundles.toString,
        "numSheafInMemory" -> bundleToSheaf.size.toString,
        "numTXInMemory" -> txHashToTX.size.toString,
        "numValidBundleHashesRemovedFromMemory" -> numValidBundleHashesRemovedFromMemory.toString,
        "udpPacketGroupSize" -> udpPacketGroupSize.toString,
        "address" -> selfAddress.address,
        "balance" -> (selfIdBalance
          .getOrElse(0L) / Schema.NormalizationFactor).toString,
        "id" -> id.b58,
        "z_keyPair" -> keyPair.json,
        "shortId" -> id.short,
        "last1000BundleHashSize" -> last100ValidBundleMetaData.size.toString,
        "numSyncedTX" -> numSyncedTX.toString,
        "numP2PMessages" -> totalNumP2PMessages.toString,
        "numSyncedBundles" -> numSyncedBundles.toString,
        "numValidBundles" -> totalNumValidBundles.toString,
        "numValidTransactions" -> totalNumValidatedTX.toString,
        "memPoolSize" -> memPool.size.toString,
        "totalNumBroadcasts" -> totalNumBroadcastMessages.toString,
        "totalNumBundleMessages" -> totalNumBundleMessages.toString,
        "lastConfirmationUpdateTime" -> lastConfirmationUpdateTime.toString,
        "numPeers" -> peers.size.toString,
        "peers" -> peers
          .map { z =>
            val addr = z.data.apiAddress.map {
              a => s"http://${a.getHostString}:${a.getPort}"
            }.getOrElse("")
            s"${z.data.id.short} API: $addr"
          }
          .mkString(" --- "),
        "z_peerSync" -> peerSync.toMap.toString,
        "z_peerLookup" -> signedPeerLookup.toMap.toString,
        "downloadInProgress" -> downloadInProgress.toString,
        "z_genesisBundleHash" -> genesisBundle
          .map {
            _.hash
          }
          .getOrElse("N/A"),
        //   "bestBundleCandidateHashes" -> bestBundleCandidateHashes.map{_.hash}.mkString(","),
        "numActiveBundles" -> activeDAGBundles.size.toString,
        "last10TXHash" -> last10000ValidTXHash.
          takeRight(10)
          .mkString(","),
        "last10ValidBundleHashes" -> last100ValidBundleMetaData
          .map {
            _.bundle.hash
          }
          .takeRight(10)
          .mkString(","),
        "last10SelfTXHashes" -> last100SelfSentTransactions
          .map {
            _.hash
          }
          .takeRight(10)
          .mkString(","),
        "lastValidBundleHash" -> Try {
          lastValidBundleHash.pbHash
        }.getOrElse(""),
        "lastValidBundle" -> Try {
          Option(lastValidBundle)
            .map {
              _.pretty
            }
            .getOrElse("")
        }.getOrElse(""),
        "z_genesisBundle" -> genesisBundle.map(_.json).getOrElse(""),
        "z_genesisBundleIds" -> genesisBundle
          .map(_.extractIds.mkString(", "))
          .getOrElse(""),
        "selfBestBundle" -> Try {
          maxBundle.map {
            _.pretty
          }.toString
        }.getOrElse(""),
        "selfBestBundleHash" -> Try {
          maxBundle.map {
            _.hash
          }.toString
        }.getOrElse(""),
        "selfBestBundleMeta" -> Try {
          maxBundleMetaData.toString
        }.getOrElse(""),
        "reputations" -> normalizedDeterministicReputation
          .map {
            case (k, v) => k.short + " " + v
          }
          .mkString(" - "),
        "peersAwaitingAuthentication" -> peersAwaitingAuthenticationToNumAttempts.toMap
          .toString(),
        "numProcessedBundles" -> totalNumNewBundleAdditions.toString,
        "numSyncPendingBundles" -> syncPendingBundleHashes.size.toString,
        "numSyncPendingTX" -> syncPendingTXHashes.size.toString,
        "peerBestBundles" -> peerSync.toMap
          .map {
            case (id, b) =>
              Try {
                s"${id.short}: ${b.maxBundle.hash.take(5)} ${Try {
                  b.maxBundle.pretty
                }} " +
                  s"parent${b.maxBundle.extractParentBundleHash.pbHash.take(5)} " +
                  s"${lookupBundle(b.maxBundle.hash).nonEmpty} ${b.maxBundle.meta.map {
                    _.transactionsResolved
                  }}"
              }.getOrElse("")
          }
          .mkString(" --- "),
        "z_peers" -> peers.map {
          _.data
        }.json,
        "z_validLedger" -> validLedger.toMap.json,
        "z_mempoolLedger" -> memPoolLedger.toMap.json,
        "z_Bundles" -> activeDAGBundles
          .sortBy { z =>
            -1 * z.totalScore.getOrElse(0D)
          }
          .map {
            _.bundle.pretty
          }
          .mkString(" --- "),
        "downloadMode" -> downloadMode.toString,
        "allPeersHaveKnownBestBundles" -> Try {
          peerSync.forall {
            case (_, hb) =>
              lookupBundle(hb.maxBundle.hash).nonEmpty
          }.toString
        }.getOrElse(""),
        "allPeersAgreeOnValidLedger" -> Try {
          peerSync.forall {
            case (_, hb) =>
              hb.validLedger == validLedger.toMap
          }.toString
        }.getOrElse(""),
        "allPeersHaveResolvedMaxBundles" -> Try {
          peerSync.forall {
            _._2.safeMaxBundle.exists {
              _.meta.exists(_.isResolved)
            }
          }.toString
        }.getOrElse(""),
        "allPeersAgreeWithMaxBundle" -> Try {
          peerSync.forall {
            _._2.maxBundle == maxBundle.get
          }.toString
        }.getOrElse("")
        //,
        // "z_lastBundleVisualJSON" -> Option(lastBundle).map{ b => b.extractTreeVisual.json}.getOrElse("")
      ))
  }

  //  def getMetrics() = {
  //    memoizeSync[Try, Metrics](Some(2.seconds))(2)
  //  }

  val getEndpoints: Route = cors() {
    extractClientIP { clientIP =>
      //  numAPICalls += 1
      /*      logger.debug(s"Client IP " +
              s"$clientIP ${clientIP.getAddress()} " +
              s"${clientIP.toIP} ${clientIP.toOption.map{_.getCanonicalHostName}} ${clientIP.getPort()}"
            )*/
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
          pathPrefix("address") {
            get {
              extractUnmatchedPath { p =>
                logger.debug(s"Unmatched path on address result $p")
                val ps = p.toString().tail
                val balance = validLedger.getOrElse(ps, 0).toString
                complete(s"Balance: $balance")
              }
            }
          } ~
          pathPrefix("txHash") { // TODO: Rename to transaction
            extractUnmatchedPath { p =>
              logger.debug(s"Unmatched path on txHash result $p")
              val ps = p.toString().tail
              complete(lookupTransactionDB(ps).prettyJson)
            }
          } ~
          pathPrefix("transaction") {
            extractUnmatchedPath { p =>
              //   logger.debug(s"Unmatched path on address result $p")
              val ps = p.toString().tail
              complete(lookupTransactionDB(ps))
            }
          } ~
          pathPrefix("bundle") {
            extractUnmatchedPath { p =>
              logger.debug(s"Unmatched path on bundle direct result $p")
              val ps = p.toString().tail

              //findAncestorsUpTo()
              val maybeSheaf = lookupBundleDBFallbackBlocking(ps)
              complete(maybeSheaf)
            }
          } ~
          pathPrefix("fullBundle") {
            extractUnmatchedPath { p =>
              logger.debug(s"Unmatched path on fullBundle result $p")
              val ps = p.toString().split("/").last
              val maybeSheaf = lookupBundle(ps)
              complete(
                BundleHashQueryResponse(
                  ps,
                  maybeSheaf,
                  maybeSheaf
                    .map(_.bundle.extractTX.toSeq.sortBy {
                      _.txData.time
                    })
                    .getOrElse(Seq())
                )
              )
            }
          } ~ pathPrefix("download") {
          extractUnmatchedPath { p =>
            Try {
              logger.debug(s"Unmatched path on download result $p")
              val ps = p.toString().split("/").last
              //logger.debug(s"Looking up bundle hash $ps")
              val ancestors = findAncestorsUpTo(ps, Seq(), upTo = 10)
              //logger.debug(s"Found ${ancestors.size} ancestors : $ancestors")
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
              //logger.debug(s"Looking up bundle hash $ps")
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
            complete(calculateMetrics())
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
            //  val pair = constellation.makeKeyPair()
            //  wallet :+= pair
            //  complete(constellation.pubKeyToAddress(pair.getPublic))
            complete(id.address)
          } ~
          path("id") {
            complete(id)
          } ~
          path("nodeKeyPair") {
            complete(keyPair)
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
          path("balance") {
            entity(as[PublicKey]) { account =>
              logger.debug(
                s"Received request to query account $account balance")
              // TODO: update balance
              // complete((chainStateActor ? GetBalance(account)).mapTo[Balance])

              complete(StatusCodes.OK)
            }
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
                      t._2))
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
  }

  private val postEndpoints =
    post {
      //    numAPICalls += 1 // <- This breaks literally everything.
      path("startRandomTX") {
        sendRandomTXV2 = !sendRandomTXV2
        complete(sendRandomTXV2.toString)
      } ~
      path("sendV2") {
        entity(as[SendToAddress]) { e =>
          nodeManager ! e
          complete(StatusCodes.OK)
        }
      } ~
      path("peerHealthCheckV2") {
        val response = (peerManager ? APIBroadcast(_.get("health"))).mapTo[Map[Id, Future[HttpResponse]]]
        val res = response.getOpt().map{
          idMap =>
            val res = idMap.map{
              case (id, fut) =>
                val maybeResponse = fut.getOpt()
             //   println(s"Maybe response $maybeResponse")
                id -> maybeResponse.exists{_.status == StatusCodes.OK}
            }.toSeq
            complete(res)
        }.getOrElse(complete(StatusCodes.InternalServerError))
        res
      } ~
        path("acceptGenesisOE") {
          entity(as[GenesisObservation]) { go =>

            dbActor.foreach { db =>
              go.genesis.store(db)
              go.initialDistribution.store(db)
              val gtx = go.genesis.resolvedTX.head.transactionData
              val distributionTX = go.genesis.resolvedTX.toSeq.map{_.transactionData}
              db ! DBPut(gtx.dst, AddressCache(gtx.amount - distributionTX.map{_.amount}.sum, Some(1000D)))
              distributionTX.foreach{ t =>
                db ! DBPut(t.dst, AddressCache(t.amount, Some(1000D)))
              }
              metricsManager ! UpdateMetric("validTransactions", (1 + distributionTX.size).toString)
              metricsManager ! UpdateMetric("uniqueAddressesInLedger", (1 + distributionTX.size).toString)
              cellManager ! go

            }

            complete(StatusCodes.OK)
          }
        } ~
        path("genesisObservation") {
          entity(as[Set[Id]]) { ids =>
            complete(createGenesisAndInitialDistributionOE(ids))
          }
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
          entity(as[Bundle]) { b =>
    //        println("api handle bundle")
            //handleBundle(b)
           peerToPeerActor ! b
            complete(StatusCodes.OK)
          }
        } ~
        path("handleTransaction") {
          entity(as[Transaction]) { tx =>
            peerToPeerActor ! Transaction
            /*
            if (lookupTransaction(tx.hash).isEmpty) {
              storeTransaction(tx)
              numSyncedTX += 1
            }
            syncPendingTXHashes -= tx.hash
            txSyncRequestTime.remove(tx.hash)*/
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
            //     println("API batch tx request.")
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
          entity(as[Transaction]) { tx =>
            peerToPeerActor ! tx
            complete(StatusCodes.OK)
          }
        } ~
        path("addPeerV2"){
          entity(as[AddPeerRequest]) { e =>

         //   println("addpeerv2")
            peerManager ! e
            /*askRes.mapTo[HashSignature].getOpt().map{
              complete(_)
            }.getOrElse(complete(StatusCodes.InternalServerError))*/
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

  private val noAuthRoutes =
    get {
      // TODO: revisit
      path("health") {
        complete(StatusCodes.OK)
      } ~
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

  val authRoutes = noAuthRoutes ~ authenticateBasic(realm = "secure site",
                                                    myUserPassAuthenticator) {
    user =>
      routes
  }

}
