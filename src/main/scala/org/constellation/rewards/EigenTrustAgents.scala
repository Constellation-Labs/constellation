package org.constellation.rewards

import java.security.SecureRandom
import org.constellation.rewards.EigenTrustAgents.BiDirectionalMap
import org.constellation.schema.Id

import scala.collection.immutable.Map

object EigenTrustAgents {
  type BiDirectionalMap[A, B] = (Map[A, B], Map[B, A])

  def empty(): EigenTrustAgents = EigenTrustAgents((Map.empty[Id, Int], Map.empty[Int, Id]))
}

/**
  * We need to pass Ids as agents to EigenTrust. Each agent is passed as Int but stringified Id is too long
  * to fit Int's size. Therefore we store the mappings between Id and random Int to make a bridge between
  * the https://github.com/djelenc/alpha-testbed EigenTrust and our app
  */
case class EigenTrustAgents(private val agentMappings: BiDirectionalMap[Id, Int] = (Map.empty[Id, Int], Map.empty[Int, Id])) {
  private val secureRandom = SecureRandom.getInstanceStrong

  def registerAgent(id: Id): EigenTrustAgents = EigenTrustAgents(update(id, secureRandom.nextInt))

  def unregisterAgent(agent: Int): EigenTrustAgents = EigenTrustAgents(remove(agent))
  def unregisterAgent(agent: Id): EigenTrustAgents = EigenTrustAgents(remove(agent))

  def get(agent: Int): Option[Id] =
    agentMappings match {
      case (_, intToId: Map[Int, Id]) => intToId.get(agent)
    }
  def get(id: Id): Option[Int] =
    agentMappings match  {
      case (idToInd: Map[Id, Int], _) => idToInd.get(id)
    }

  def contains(agent: Int): Boolean = get(agent).isDefined
  def contains(id: Id): Boolean = get(id).isDefined

  private def update(key: Id, value: Int): BiDirectionalMap[Id, Int] = {
    agentMappings match {
      case (idToInt: Map[Id, Int], intToId: Map[Int, Id]) =>
        (idToInt + (key -> value), intToId + (value -> key))
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