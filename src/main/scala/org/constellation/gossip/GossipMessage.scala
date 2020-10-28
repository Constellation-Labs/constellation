package org.constellation.gossip

case class GossipMessage[A](data: A, path: GossipPath)
