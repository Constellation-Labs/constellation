package org.constellation.primitives

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.primitives.IPManager.IP
import org.constellation.primitives.concurrency.SingleRef

import scala.collection.Set

class IPManager[F[_]: Concurrent] {
  private val bannedIPs: SingleRef[F, Set[IP]] = SingleRef(Set.empty[IP])
  private val knownIPs: SingleRef[F, Set[IP]] = SingleRef(Set.empty[IP])

  def knownIP(addr: IP): F[Boolean] =
    knownIPs.getUnsafe.map(_.contains(addr))

  def bannedIP(addr: String): F[Boolean] =
    bannedIPs.getUnsafe.map(_.contains(addr))

  def listBannedIPs(): F[Set[IP]] =
    bannedIPs.getUnsafe

  def addKnownIP(addr: IP): F[Unit] =
    knownIPs.modify(ips => (ips + addr, ()))

  def removeKnownIP(addr: IP): F[Unit] =
    knownIPs.modify(ips => (ips - addr, ()))

  def listKnownIPs: F[Set[IP]] =
    knownIPs.getUnsafe
}

object IPManager {
  def apply[F[_]: Concurrent]() = new IPManager[F]()

  type IP = String
}
