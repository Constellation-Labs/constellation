package org.constellation.primitives
import akka.http.scaladsl.model.RemoteAddress

import scala.collection.{Set, concurrent}

class IPManager {
  private var bannedIPs: concurrent.Map[RemoteAddress, String] =
    concurrent.TrieMap[RemoteAddress, String]()
  //    val knownIPs: concurrent.Map[RemoteAddress, String] =
  //      concurrent.TrieMap[RemoteAddress, String]()
  private var knownIPs: Set[RemoteAddress] = Set()

  def knownIP(addr: RemoteAddress): Boolean = {
    knownIPs.contains(addr)
  }

  def bannedIP(addr: RemoteAddress): Boolean = {
    bannedIPs.contains(addr)
  }

  def addKnownIp(addr: RemoteAddress) = {
    knownIPs = knownIPs + addr
  }

  def removeKnownIp(addr: RemoteAddress) = {
    knownIPs = knownIPs - addr
  }
}

object IPManager {
  def apply() = new IPManager()
}