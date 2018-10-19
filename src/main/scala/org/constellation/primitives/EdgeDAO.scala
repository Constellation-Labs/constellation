package org.constellation.primitives

import java.util.concurrent.Executors

import org.constellation.ProcessingConfig
import org.constellation.primitives.Schema._

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

trait EdgeDAO {

  var snapshotInterval: Int = 30

  var genesisObservation: Option[GenesisObservation] = None
  def maxWidth: Int = processingConfig.maxWidth
  def minCheckpointFormationThreshold: Int = processingConfig.minCheckpointFormationThreshold
  def minCBSignatureThreshold: Int = processingConfig.minCBSignatureThreshold

  val minTXSignatureThreshold = 5
  val maxUniqueTXSize = 500
  val maxNumSignaturesPerTX = 20

  var processingConfig = ProcessingConfig()

  @volatile var transactionMemPool: Seq[Transaction] = Seq()

  val transactionMemPoolMultiWitness : TrieMap[String, Transaction] = TrieMap()
  val checkpointMemPool : TrieMap[String, CheckpointBlock] = TrieMap()

  // TODO: temp
  val confirmedCheckpoints: TrieMap[String, CheckpointBlock] = TrieMap()

  @volatile var transactionMemPoolThresholdMet: Set[String] = Set()

  // Map from checkpoint hash to number of times used as a tip (number of children)
  val checkpointMemPoolThresholdMet: TrieMap[String, (CheckpointBlock, Int)] = TrieMap()



  val resolveNotifierCallbacks: TrieMap[String, Seq[CheckpointBlock]] = TrieMap()

  val edgeExecutionContext: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(20))

  def canCreateCheckpoint: Boolean = {
    transactionMemPool.size >= minCheckpointFormationThreshold && checkpointMemPoolThresholdMet.size >= 2
  }

  def reuseTips: Boolean = checkpointMemPoolThresholdMet.size < maxWidth


  // Temporary to get peer data for tx hash partitioning
  @volatile var peerInfo: Map[Id, PeerData] = Map()


}
