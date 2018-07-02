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
  @volatile var numSyncedBundles: Int = 0
  @volatile var numSyncedTX: Int = 0
  @volatile var heartbeatRound = 0L

}
