package org.constellation.primitives

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.util.Timeout
import org.constellation.primitives.Schema.CheckpointBlock

trait EdgeExt extends NodeData with Ledger with MetricsExt with PeerInfo with EdgeDAO {
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
  def markParentsUnresolved(missingParentHash: String, cb: CheckpointBlock) =
    resolveNotifierCallbacks.get(missingParentHash) match {
      case Some(cbs) if !cbs.contains(cb) =>
        resolveNotifierCallbacks(missingParentHash) :+= cb
      case None =>
        queryMissingResolutionData(missingParentHash, cb.signatures.map {
          _.toId
        }.toSet)
        resolveNotifierCallbacks(missingParentHash) = Seq(cb)
    }
}