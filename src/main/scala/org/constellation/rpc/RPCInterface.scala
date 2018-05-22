package org.constellation.rpc

import java.io.File
import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import akka.actor.ActorRef
import akka.http.scaladsl.marshalling.Marshaller._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, PredefinedFromEntityUnmarshallers}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import de.heikoseeberger.akkahttpjson4s.Json4sSupport
import org.constellation.LevelDB
import org.constellation.p2p.PeerToPeer._
import org.constellation.primitives.Schema._
import org.constellation.primitives.{Block, BlockSerialized, Schema, Transaction}
import org.constellation.state.ChainStateManager.{CurrentChainStateUpdated, GetCurrentChainState}
import org.constellation.state.MemPoolManager.AddTransaction
import org.json4s.native.Serialization
import org.json4s.{Formats, native}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import constellation._

class RPCInterface(
                    chainStateActor: ActorRef,
                    peerToPeerActor: ActorRef,
                    memPoolManagerActor: ActorRef,
                    consensusActor: ActorRef,
                    udpAddress: InetSocketAddress,
                    keyPair: KeyPair = null,
                    db: LevelDB = null,
                    val jsPrefix: String = "./ui/target/scala-2.11/ui"
                  )
                  (implicit executionContext: ExecutionContext, timeout: Timeout) extends Json4sSupport {

  implicit val id : Id = Id(keyPair.getPublic)

  private val selfAddress = id.address

  implicit val serialization: Serialization.type = native.Serialization

  implicit val stringUnmarshaller: FromEntityUnmarshaller[String] =
    PredefinedFromEntityUnmarshallers.stringUnmarshaller

  val logger = Logger(s"RPCInterface")

  // TODO: Not this.
  @volatile var wallet : Seq[KeyPair] = Seq()

  private def walletPair = {
    val pair = constellation.makeKeyPair()
    wallet :+= pair
    pair
  }

  def addresses: Seq[String] = wallet.map{_.address.address}

  def addressToKeyPair: Map[String, KeyPair] = wallet.map{ w => w.address.address -> w}.toMap

  private def walletAddressInfo = {
    wallet.map{_.address.address}.flatMap{k => db.getAs[TX](k).map{k -> _}}.toMap
  }

  def outputBalances: Seq[Address] = walletAddressInfo.flatMap{
    case (k,v) =>
      v.output(k)
  }.toSeq

  private def UTXO = (peerToPeerActor ? GetUTXO).mapTo[Map[String, Long]].get()
  private def memPoolUTXO = (peerToPeerActor ? GetMemPoolUTXO).mapTo[Map[String, Long]].get()

  def selfIdBalance: Option[Long] = memPoolUTXO.get(selfAddress.address)

  def utxoBalance: Map[String, Long] = {
    UTXO.filter{case (x,y) => addresses.contains(x)}
  }

  val routes: Route =
    get {
      path("walletAddressInfo") {
        complete(walletAddressInfo)
      } ~
      path("balances") {
        complete(outputBalances)
      } ~
      path("makeKeyPair") {
        val pair = constellation.makeKeyPair()
        wallet :+= pair
        complete(pair)
      } ~
      path("genesis" / LongNumber) { numCoins =>

          val debtAddress = walletPair
       //   val dstAddress = walletPair

          val amount = numCoins * Schema.NormalizationFactor
          val tx = TX(
            TXData(
              Seq(debtAddress.address.copy(balance = -1*amount)),
              selfAddress,
              amount,
              genesisTXHash = None,
              isGenesis = true
            ).signed()(debtAddress)
          )

          peerToPeerActor ! tx
          complete(tx.json)
        } ~
        path("makeKeyPairs" / IntNumber) { numPairs =>
        val pair = Seq.fill(numPairs){constellation.makeKeyPair()}
        wallet ++= pair
        complete(pair)
      } ~
      path("wallet") {
        complete(wallet)
      } ~
      path("address") {
        val pair = constellation.makeKeyPair()
        wallet :+= pair
        complete(constellation.pubKeyToAddress(pair.getPublic))
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
      path("blocks") {
        val future = chainStateActor ? GetCurrentChainState

        val responseFuture: Future[CurrentChainStateUpdated] = future.mapTo[CurrentChainStateUpdated]

        val chainStateUpdated = Await.result(responseFuture, timeout.duration)

        val chain = chainStateUpdated.chain

        val blocks = chain.chain

        complete(blocks)
      } ~
      path("peers") {
        complete((peerToPeerActor ? GetPeers).mapTo[Peers])
      } ~
      path("peerids") {
        complete((peerToPeerActor ? GetPeersData).mapTo[Seq[Peer]])
      } ~
      path("id") {
        complete((peerToPeerActor ? GetId).mapTo[Id])
      } ~ path("actorPath") {
        complete(peerToPeerActor.path.toSerializationFormat)
      } ~
      path("balance") {
        entity(as[PublicKey]) { account =>
          logger.debug(s"Received request to query account $account balance")

          // TODO: update balance
          // complete((chainStateActor ? GetBalance(account)).mapTo[Balance])

          complete(StatusCodes.OK)
        }
      } ~
        pathPrefix("ui") {
          get {
            extractUnmatchedPath { path =>
              logger.info(s"UI Request $path")
              getFromFile(new File(jsPrefix + path.toString))
            }
          }
        } ~
               complete { //complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1>Say hello to akka-http</h1>"))
                 logger.info("Serve main page")
                 val html = """<!DOCTYPE html>
                              |<html lang="en">
                              |<head>
                              |    <meta charset="UTF-8">
                              |    <title>Constellation</title>
                              |</head>
                              |<body style="background-color:#060613">
                              |<script src="ui-fastopt.js" type="text/javascript"></script>
                              |<script type="text/javascript">
                              |org.constellation.ui.App().main()
                              |</script>
                              |</body>
                              |</html>""".stripMargin.replaceAll("\n", "")
                 logger.info("Serving: " + html)

                 val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, html)
                 HttpResponse(entity = entity)
                 //getFromResource("index.html",ContentTypes.`text/html(UTF-8)`)
               }
          } ~
    post {
      path ("sendToAddress") {
        entity(as[SendToAddress]) { s =>

          val srcAddress = selfAddress
          val dstAddress = s.address

          val tx = TX(
            TXData(
              Seq(srcAddress), dstAddress, s.amountActual
            ).signed()(keyPair)
          )

          logger.info(s"SendToAddress RPC Transaction: ${tx.pretty}")

          peerToPeerActor ! tx

          complete(tx) //, StatusCodes.Accepted)

          // Do full UTXO later, simplifying it for now to use 1 address.
/*          val ut = utxoBalance
          val (addressWithSufficientBalance, prvBalance) = ut.filter{_._2 > s.amountActual}.head
          val txAssociated = walletAddressInfo(addressWithSufficientBalance)
          val addressMeta = txAssociated.output(addressWithSufficientBalance).get
          val ukp = addressToKeyPair(addressWithSufficientBalance)
          val remainder = walletPair


          val remainderBalance = prvBalance - s.amountActual

          println(s"Send To Address $prvBalance $addressWithSufficientBalance $addressMeta $txAssociated")

          val genHash = if (txAssociated.tx.data.genesisTXHash.isEmpty && txAssociated.tx.data.isGenesis)
            Some(txAssociated.hash) else txAssociated.tx.data.genesisTXHash

          // TODO: Fix hashes.
          val txD = TXData(
            Seq(addressMeta.copy(
              balance = 0L,
              txHashPool = addressMeta.txHashPool :+ txAssociated.hash
            )),
            s.address.copy(
              balance = s.amountActual
            ),
            s.amountActual,
            remainder = Some(remainder.address.copy(
              balance = remainderBalance
            )),
            srcAccount = Some(id.id),
            dstAccount = s.account,
            genesisTXHash = genHash
          )

          val tx = TX(txD.multiSigned()(Seq(ukp, keyPair)))
*/

     //   complete(StatusCodes.OK)

        }
      } ~
      path("db") {
        entity(as[String]){ e: String =>
          import constellation.EasyFutureBlock
          val res = (peerToPeerActor ? DBQuery(e.replaceAll('"'.toString, ""))).mapTo[Option[String]].get()
          complete(res)
      }
      } ~
      path ("tx") {
        entity(as[TX]) { tx =>
          peerToPeerActor ! tx
          complete(StatusCodes.OK)
        }
      } ~
      path("transaction") {
        entity(as[Transaction]) { transaction =>
        //  logger.debug(s"Received request to submit a new transaction $transaction")

          val tx = AddTransaction(transaction)
          peerToPeerActor ! tx
          memPoolManagerActor ! tx
          complete(transaction)
        }
      } ~
      path("peer") {
        entity(as[String]) { peerAddress =>
      //    logger.debug(s"Received request to add a new peer $peerAddress")
          val result = Try {
            peerAddress.replaceAll('"'.toString,"").split(":") match {
              case Array(ip, port) => new InetSocketAddress(ip, port.toInt)
              case a@_ => logger.debug(s"Unmatched Array: $a"); throw new RuntimeException(s"Bad Match: $a");
            }
          }.toOption match {
            case None =>
              StatusCodes.BadRequest
            case Some(v) =>
              val fut = (peerToPeerActor ?  AddPeerFromLocal(v)).mapTo[StatusCode]
              val res = Try{Await.result(fut, timeout.duration)}.toOption
              res match {
                case None =>
                  StatusCodes.RequestTimeout
                case Some(f) =>
                  if (f == StatusCodes.Accepted) {
                    var attempts = 0
                    var peerAdded = false
                    while (attempts < 5) {
                      attempts += 1
                      Thread.sleep(500)
                      import constellation.EasyFutureBlock
                      val peers = (peerToPeerActor ? GetPeersData).mapTo[Seq[Peer]].get()
                   //   peers.foreach{println}
                   //   println(v)
                      peerAdded = peers.exists(p => v == p.externalAddress)
                    }
                    if (peerAdded) StatusCodes.OK else StatusCodes.NetworkConnectTimeout
                  } else f
              }
          }

      //    logger.debug(s"New peer request $peerAddress statusCode: $result")
          complete(result)
        }
      } ~
      path("ip") {
        entity(as[String]) { externalIp =>
          val addr = externalIp.replaceAll('"'.toString,"").split(":") match {
            case Array(ip, port) => new InetSocketAddress(ip, port.toInt)
            case a@_ => { logger.debug(s"Unmatched Array: $a"); throw new RuntimeException(s"Bad Match: $a"); }
          }
          logger.debug(s"Set external IP RPC request $externalIp $addr")
          peerToPeerActor ! SetExternalAddress(addr)
          complete(StatusCodes.OK)
        }
      }
    }
}