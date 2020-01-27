package org.constellation.rewards

import java.security.SecureRandom
import org.constellation.rewards.EigenTrustAgents.BiDirectionalMap
import org.constellation.schema.Id

import scala.collection.immutable.Map

object EigenTrustAgents {
  type BiDirectionalMap[A, B] = (Map[A, B], Map[B, A])

  val iterator: AgentsIterator = AgentsIterator()

  def empty(): EigenTrustAgents = EigenTrustAgents((Map.empty[String, Int], Map.empty[Int, String]))
}

case class AgentsIterator() extends Iterator[Int] {
  var current: Int = 0;

  override def hasNext: Boolean = current != Int.MaxValue

  override def next(): Int = {
    if (hasNext) {
      current = current + 1
      current
    } else
      -1
  }
}

/**
  * We need to pass Ids as agents to EigenTrust. Each agent is passed as Int but stringified Id is too long
  * to fit Int's size. Therefore we store the mappings between Id and random Int to make a bridge between
  * the https://github.com/djelenc/alpha-testbed EigenTrust and our app
  */
case class EigenTrustAgents(private val agentMappings: BiDirectionalMap[String, Int] = (Map.empty[String, Int], Map.empty[Int, String])) {

  def registerAgent(address: String): EigenTrustAgents = {
    if (contains(address)) {
      this.copy(agentMappings)
    } else {
      this.copy(update(address, EigenTrustAgents.iterator.next()))
    }
  }

  def unregisterAgent(agent: Int): EigenTrustAgents = this.copy(remove(agent))
  def unregisterAgent(agent: String): EigenTrustAgents = this.copy(remove(agent))

  def get(agent: Int): Option[String] =
    agentMappings match {
      case (_, intToAddress: Map[Int, String]) => intToAddress.get(agent)
    }
  def get(address: String): Option[Int] =
    agentMappings match  {
      case (addressToInt: Map[String, Int], _) => addressToInt.get(address)
    }

  def getUnsafe(address: String): Int = get(address).get
  def getUnsafe(agent: Int): String = get(agent).get

  def getAllAsAddresses(): Map[String, Int] = agentMappings._1
  def getAllAsInts(): Map[Int, String] = agentMappings._2

  def contains(agent: Int): Boolean = get(agent).isDefined
  def contains(address: String): Boolean = get(address).isDefined

  def clear(): EigenTrustAgents = EigenTrustAgents.empty

  private def update(key: String, value: Int): BiDirectionalMap[String, Int] = {
    agentMappings match {
      case (addressToInt: Map[String, Int], intToAddress: Map[Int, String]) if !contains(key) && !contains(value) =>
        (addressToInt + (key -> value), intToAddress + (value -> key))
      case _ => agentMappings
    }
  }

  private def remove(agent: Int): BiDirectionalMap[String, Int] = {
    agentMappings match {
      case (addressToInt: Map[String, Int], intToAddress: Map[Int, String]) =>
        (addressToInt - intToAddress(agent), intToAddress - agent)
    }
  }

  private def remove(id: String): BiDirectionalMap[String, Int] = {
    agentMappings match {
      case (idToInt: Map[String, Int], intToId: Map[Int, String]) =>
        (idToInt - id, intToId - idToInt(id))
    }
  }
}