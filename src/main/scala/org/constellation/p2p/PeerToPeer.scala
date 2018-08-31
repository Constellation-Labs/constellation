package org.constellation.p2p

import java.security.PublicKey

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import org.constellation.Data
import org.constellation.consensus.Consensus._
import org.constellation.primitives.Schema.{TransactionV1, _}
import org.constellation.util.Heartbeat
import constellation._

import scala.concurrent.ExecutionContextExecutor

case class PeerBroadcast()

class PeerToPeer(
                  val publicKey: PublicKey,
                  system: ActorSystem,
                  val consensusActor: ActorRef,
                  val udpActor: ActorRef,
                  val data: Data = null,
                  var requestExternalAddressCheck: Boolean = false,
                  val heartbeatEnabled: Boolean = false,
                  randomTransactionManager: ActorRef,
                  cellManager: ActorRef
                )
                (implicit timeoutI: Timeout, materialize: ActorMaterializer) extends Actor
  with ActorLogging
  with PeerAuth
  with Heartbeat
  with ProbabilisticGossip
  with Download {

  import data._

  implicit val timeout: Timeout = timeoutI
  implicit val executionContext: ExecutionContextExecutor = context.system.dispatcher
  implicit val actorMaterializer: ActorMaterializer = materialize
  implicit val actorSystem: ActorSystem = context.system

  val logger = Logger(s"PeerToPeer")

  var lastTPSCheckTime: Long = System.currentTimeMillis()
  var lastNumValidTX: Long = 0L
  // Check TPS every 100 seconds
  val tpsCheckIntervalSeconds = 100

  def tpsCalculate(): Unit = {
    if (System.currentTimeMillis() > (lastTPSCheckTime + (tpsCheckIntervalSeconds*1000))) {
      lastTPSCheckTime = System.currentTimeMillis()
      val delta = Math.max(data.totalNumValidatedTX - lastNumValidTX, 1) // safety for divide by zero
      transactionsPerSecond = delta.toDouble / 100
      lastNumValidTX = data.totalNumValidatedTX
    }
  }

  override def receive: Receive = {

    // Regular state checks
    case InternalHeartbeat =>

      tpsCalculate()

      // TODO: extract to test context, for now just randomly sending txs
      if (sendRandomTXV2) {
        randomTransactionManager ! InternalHeartbeat
        cellManager ! InternalHeartbeat
        data.metricsManager ! InternalHeartbeat
      }

  }
}

