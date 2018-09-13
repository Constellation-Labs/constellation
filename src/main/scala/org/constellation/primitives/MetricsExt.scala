package org.constellation.primitives

trait MetricsExt {

  @volatile var totalNumGossipMessages = 0
  @volatile var totalNumBundleMessages = 0
  @volatile var totalNumBundleHashRequests = 0
  @volatile var totalNumInvalidBundles = 0
  @volatile var totalNumValidBundles = 0
  @volatile var totalNumNewBundleAdditions = 0
  @volatile var totalNumBroadcastMessages = 0
  @volatile var totalNumValidatedTX = 0
  @volatile var numSyncedBundles = 0
  @volatile var numSyncedTX = 0
  @volatile var heartbeatRound = 0L
  @volatile var totalNumP2PMessages = 0L
  @volatile var udpPacketGroupSize = 0L
  @volatile var numDeletedBundles = 0L
  @volatile var numTXRemovedFromMemory = 0L
  @volatile var numDBPuts = 0L
  @volatile var numDBGets = 0L
  @volatile var numDBUpdates = 0L
  @volatile var numDBDeletes = 0L
  @volatile var numMempoolEmits = 0L
  @volatile var numValidBundleHashesRemovedFromMemory = 0L
  @volatile var numAPICalls = 0L
  @volatile var allPeersHealthy = true
  @volatile var transactionsPerSecond = 1D

  def resetMetrics(): Unit = {

    totalNumGossipMessages = 0
    totalNumBundleMessages = 0
    totalNumBundleHashRequests = 0
    totalNumInvalidBundles = 0
    totalNumValidBundles = 0
    totalNumNewBundleAdditions = 0
    totalNumBroadcastMessages = 0
    totalNumValidatedTX = 0
    numSyncedBundles = 0
    numSyncedTX = 0
    heartbeatRound = 0L
    totalNumP2PMessages = 0L
    udpPacketGroupSize = 0L
    numDeletedBundles = 0L
    numTXRemovedFromMemory = 0L
    numDBPuts = 0L
    numDBGets = 0L
    numDBUpdates = 0L
    numDBDeletes = 0L
    numMempoolEmits = 0L
    numValidBundleHashesRemovedFromMemory = 0L
    transactionsPerSecond = 1D

  }

}
