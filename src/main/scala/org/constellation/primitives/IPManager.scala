package org.constellation.primitives

import akka.http.scaladsl.model.RemoteAddress
import scala.collection.{Set, concurrent}

/** IP manager class. */
class IPManager {

  // Keep these private to allow for change of implementation later.
  private var bannedIPs: concurrent.Map[RemoteAddress, String] = concurrent.TrieMap[RemoteAddress, String]()

  //val knownIPs: concurrent.Map[RemoteAddress, String] = concurrent.TrieMap[RemoteAddress, String]() // tmp comment

  private var knownIPs: Set[RemoteAddress] = Set()

  /** @return Whether in input address is among the known IP's. */
  def knownIP(addr: RemoteAddress): Boolean = {
    knownIPs.contains(addr)
  }

  /** @return Whether in input address is among the banned IP's. */
  def bannedIP(addr: RemoteAddress): Boolean = {
    bannedIPs.contains(addr)
  }

  /** @return The dictonary of banned IP's. */
  def listBannedIPs = bannedIPs

  /** Adds input addres to the known IP's. */
  def addKnownIP(addr: RemoteAddress) = {
    knownIPs = knownIPs + addr
  }

  /** Remove the input addres from the known IP's. */
  def removeKnownIP(addr: RemoteAddress) = {
    knownIPs = knownIPs - addr
  }

  /** @return The known IP's. */
  def listKnownIPs = knownIPs

} // class IPManager

/** IP manager companion object. */
object IPManager {

  /** Apply call. */
  def apply() = new IPManager()

}
