package org.constellation.primitives
import akka.http.scaladsl.model.RemoteAddress

import scala.collection.{Set, concurrent}

/** Documentation. */
class IPManager {
  // Keep these private to allow for change of implementation later.
  private var bannedIPs: concurrent.Map[RemoteAddress, String] =
    concurrent.TrieMap[RemoteAddress, String]()
  //    val knownIPs: concurrent.Map[RemoteAddress, String] =
  //      concurrent.TrieMap[RemoteAddress, String]()
  private var knownIPs: Set[RemoteAddress] = Set()

  /** Documentation. */
  def knownIP(addr: RemoteAddress): Boolean = {
    knownIPs.contains(addr)
  }

  /** Documentation. */
  def bannedIP(addr: RemoteAddress): Boolean = {
    bannedIPs.contains(addr)
  }

  /** Documentation. */
  def listBannedIPs = bannedIPs

  /** Documentation. */
  def addKnownIP(addr: RemoteAddress) = {
    knownIPs = knownIPs + addr
  }

  /** Documentation. */
  def removeKnownIP(addr: RemoteAddress) = {
    knownIPs = knownIPs - addr
  }

  /** Documentation. */
  def listKnownIPs = knownIPs

}

/** Documentation. */
object IPManager {

  /** Documentation. */
  def apply() = new IPManager()
}
