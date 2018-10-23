package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import org.constellation.primitives.Schema.CheckpointBlock

trait EdgeExt extends NodeData with MetricsExt with PeerInfo with EdgeDAO {

  /**
    * TODO: Need to include signatories ABOVE this checkpoint block later in the case of signature decay. Add to SignedObservationEdgeCache
    *
    * @param cb
    * @return
    */
  def downloadAncestry(cb: CheckpointBlock) = {
    val observers: Set[Schema.Id] = cb.signatures.map {
      _.toId
    }.toSet
    queryMissingResolutionData(cb.baseHash, observers)
  }

  /**
    *
    * @param h
    * @param signers
    * @return
    */
  def queryMissingResolutionData(h: String, signers: Set[Schema.Id]) = {
    implicit val timeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    peerManager ? APIBroadcast({
      apiClient =>
        apiClient.get("edge/" + h)
    }, peerSubset = signers)
  }

  /**
    *
    * @param missingParentHash
    * @param cb
    */
  def markParentsUnresolved(missingParentHash: String, cb: CheckpointBlock) = {
    val children = resolveNotifierCallbacks.get(missingParentHash)
    if (children.isDefined && children.exists(_.contains(cb)))
      children.foreach(c => resolveNotifierCallbacks.update(missingParentHash, c :+ cb))
    else {
      downloadAncestry(cb)
      resolveNotifierCallbacks.update(missingParentHash, Seq(cb))
    }
  }

}