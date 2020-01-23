package org.constellation.rewards

import java.security.SecureRandom
import org.constellation.rewards.EigenTrustAgents.BiDirectionalMap
import org.constellation.schema.Id

import scala.collection.immutable.Map

object EigenTrustAgents {
  type BiDirectionalMap[A, B] = (Map[A, B], Map[B, A])

  val iterator: AgentsIterator = AgentsIterator()

  def empty(): EigenTrustAgents = EigenTrustAgents((Map.empty[Id, Int], Map.empty[Int, Id]))
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
case class EigenTrustAgents(private val agentMappings: BiDirectionalMap[Id, Int] = (Map.empty[Id, Int], Map.empty[Int, Id])) {

  def registerAgent(id: Id): EigenTrustAgents = {
    if (contains(id)) {
      this.copy(agentMappings)
    } else {
      this.copy(update(id, EigenTrustAgents.iterator.next()))
    }
  }

  def unregisterAgent(agent: Int): EigenTrustAgents = this.copy(remove(agent))
  def unregisterAgent(agent: Id): EigenTrustAgents = this.copy(remove(agent))

  def get(agent: Int): Option[Id] =
    agentMappings match {
      case (_, intToId: Map[Int, Id]) => intToId.get(agent)
    }
  def get(id: Id): Option[Int] =
    agentMappings match  {
      case (idToInd: Map[Id, Int], _) => idToInd.get(id)
    }

  def getUnsafe(id: Id): Int = get(id).get
  def getUnsafe(agent: Int): Id = get(agent).get

  def getAllAsIds(): Map[Id, Int] = agentMappings._1
  def getAllAsInts(): Map[Int, Id] = agentMappings._2

  def contains(agent: Int): Boolean = get(agent).isDefined
  def contains(id: Id): Boolean = get(id).isDefined

  def clear(): EigenTrustAgents = EigenTrustAgents.empty

  private def update(key: Id, value: Int): BiDirectionalMap[Id, Int] = {
    agentMappings match {
      case (idToInt: Map[Id, Int], intToId: Map[Int, Id]) if !contains(key) && !contains(value) =>
        (idToInt + (key -> value), intToId + (value -> key))
      case _ => agentMappings
    }
  }

  private def remove(agent: Int): BiDirectionalMap[Id, Int] = {
    agentMappings match {
      case (idToInt: Map[Id, Int], intToId: Map[Int, Id]) =>
        (idToInt - intToId(agent), intToId - agent)
    }
  }

  private def remove(id: Id): BiDirectionalMap[Id, Int] = {
    agentMappings match {
      case (idToInt: Map[Id, Int], intToId: Map[Int, Id]) =>
        (idToInt - id, intToId - idToInt(id))
    }
  }
}