package org.constellation.domain.blacklist

import cats.effect.Concurrent
import cats.effect.concurrent.Ref
import cats.syntax.all._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class BlacklistedAddresses[F[_]: Concurrent] {

  private val logger = Slf4jLogger.getLogger[F]

  private val blacklistedAddresses: Ref[F, Set[String]] = Ref.unsafe(Set("DAG2bhZv7fsmXrYbx9wNiT9wP4tuannXcZkjtKeM"))

  def get: F[Set[String]] =
    blacklistedAddresses.get

  def add(address: String): F[Unit] =
    blacklistedAddresses
      .modify(blacklist => (blacklist + address, ()))
      .flatTap(_ => logger.info(s"Address added to the blacklist : $address"))

  def addAll(addresses: List[String]): F[Unit] =
    addresses.traverse(add).void

  def contains(address: String): F[Boolean] =
    blacklistedAddresses.modify(b => (b, b.contains(address)))

  def clear: F[Unit] =
    blacklistedAddresses
      .modify(_ => (Set.empty[String], ()))
      .flatTap(_ => logger.info("BlacklistedAddresses collection has been cleared"))
}

object BlacklistedAddresses {

  def apply[F[_]: Concurrent]: BlacklistedAddresses[F] =
    new BlacklistedAddresses[F]()
}
