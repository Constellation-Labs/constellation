package org.constellation.primitives

import java.security.KeyPair

import org.constellation.datastore.Datastore
import org.constellation.primitives.Schema._
import org.constellation.util.{HashSignature, Signable}

case class Edge[+D <: Signable](
  observationEdge: ObservationEdge,
  signedObservationEdge: SignedObservationEdge,
  data: D
) {

  def baseHash: String = signedObservationEdge.signatureBatch.hash

  def parentHashes: Seq[String] = observationEdge.parents.map(_.hash)

  def parents: Seq[TypedEdgeHash] = observationEdge.parents

  def withSignatureFrom(keyPair: KeyPair): Edge[D] =
    this.copy(signedObservationEdge = signedObservationEdge.withSignatureFrom(keyPair))

  def withSignature(hs: HashSignature): Edge[D] =
    this.copy(signedObservationEdge = signedObservationEdge.withSignature(hs))

  def plus(other: Edge[_]): Edge[D] =
    this.copy(signedObservationEdge = signedObservationEdge.plus(other.signedObservationEdge))

}
