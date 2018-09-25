package org.constellation.primitives

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import akka.actor.{Actor, ActorRef}
import org.constellation.primitives.Schema.{Id, InternalHeartbeat, SendToAddress}
import akka.pattern.ask
import akka.util.Timeout
import constellation._
import org.constellation.Data

import scala.util.Random

class RandomTransactionManager(dao: Data)(
  implicit val timeout: Timeout
) extends Actor {

  val random = new Random()
  val period = 1
  val timeUnit = TimeUnit.SECONDS

  private val bufferTask = new Runnable { def run(): Unit = {
    if (dao.generateRandomTX) {
      self ! InternalHeartbeat
    }
  } }

  var heartBeatMonitor: ScheduledFuture[_] = _
  var heartBeat: ScheduledThreadPoolExecutor = _

  heartBeat = new ScheduledThreadPoolExecutor(10)
  heartBeatMonitor = heartBeat.scheduleAtFixedRate(bufferTask, 1, period, timeUnit)

  override def receive: Receive = {

    /**
      * This spawns random transactions for simulating load. For testing purposes only.
      */
    case InternalHeartbeat =>

      if (dao.transactionMemPoolMultiWitness.size < 100) {
        val peerIds = (dao.peerManager ? GetPeerInfo).mapTo[Map[Id, PeerData]].get().toSeq

        def getRandomPeer: (Id, PeerData) = peerIds(random.nextInt(peerIds.size))

        val sendRequest = SendToAddress(getRandomPeer._1.address.address, random.nextInt(10000).toLong)
        val tx = createTransaction(dao.selfAddressStr, sendRequest.dst, sendRequest.amount, dao.keyPair)
        dao.metricsManager ! IncrementMetric("signaturesPerformed")
        dao.metricsManager ! IncrementMetric("randomTransactionsGenerated")
        dao.metricsManager ! IncrementMetric("sentTransactions")

        // TODO: Change to transport layer call
        dao.peerManager ! APIBroadcast(_.put(s"transaction/${tx.edge.signedObservationEdge.signatureBatch.hash}", tx))
      }

      dao.metricsManager ! InternalHeartbeat

  }
}
