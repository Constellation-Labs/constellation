package org.constellation.primitives

import scala.collection.{Set, concurrent}

class IPManager {
  // Keep these private to allow for change of implementation later.
  private var bannedIPs: Set[String] = Set.empty[String]
  //    val knownIPs: concurrent.Map[RemoteAddress, String] =
  //      concurrent.TrieMap[RemoteAddress, String]()
  private var knownIPs: Set[String] = Set.empty[String]

  def knownIP(addr: String): Boolean = {
    knownIPs.contains(addr)
  }

  def bannedIP(addr: String): Boolean = {
    bannedIPs.contains(addr)
  }

  def listBannedIPs: Set[String] = bannedIPs

  def addKnownIP(addr: String): Unit = {
    knownIPs = knownIPs + addr
  }

  def removeKnownIP(addr: String): Unit = {
    knownIPs = knownIPs - addr
  }

  def listKnownIPs: Set[String] = knownIPs

}

object IPManager {

  def apply() = new IPManager()
}
