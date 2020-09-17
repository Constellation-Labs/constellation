package org.constellation.primitives

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.syntax.all._
import org.constellation.primitives.IPManager.IP

import scala.collection.Set

class IPManager[F[_]: Concurrent] {
  private val bannedIPs: Ref[F, Set[IP]] = Ref.unsafe(Set.empty[IP])
  private val knownIPs: Ref[F, Set[IP]] = Ref.unsafe(Set.empty[IP])

  def knownIP(addr: IP): F[Boolean] =
    knownIPs.get.map(_.contains(addr))

  def bannedIP(addr: String): F[Boolean] =
    bannedIPs.get.map(_.contains(addr))

  def listBannedIPs(): F[Set[IP]] =
    bannedIPs.get

  def addKnownIP(addr: IP): F[Unit] =
    knownIPs.modify(ips => (ips + addr, ()))

  def removeKnownIP(addr: IP): F[Unit] =
    knownIPs.modify(ips => (ips - addr, ()))

  def listKnownIPs: F[Set[IP]] =
    knownIPs.get
}

object IPManager {
  def apply[F[_]: Concurrent]() = new IPManager[F]()

  type IP = String
}
