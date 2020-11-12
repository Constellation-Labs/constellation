package org.constellation.schema.v2

import enumeratum.{CirceEnum, Enum, EnumEntry}

sealed trait NodeState extends EnumEntry

object NodeState extends Enum[NodeState] with CirceEnum[NodeState] {

  case object PendingDownload extends NodeState
  case object ReadyForDownload extends NodeState
  case object DownloadInProgress extends NodeState
  case object DownloadCompleteAwaitingFinalSync extends NodeState
  case object SnapshotCreation extends NodeState
  case object Ready extends NodeState
  case object Leaving extends NodeState
  case object Offline extends NodeState

  val values = findValues

  val all: Set[NodeState] = values.toSet

  val readyStates: Set[NodeState] = Set(NodeState.Ready, NodeState.SnapshotCreation)

  val initial: Set[NodeState] = Set(Offline, PendingDownload)

  val broadcastStates: Set[NodeState] = Set(Ready, Leaving, Offline, PendingDownload, ReadyForDownload)

  val offlineStates: Set[NodeState] = Set(Offline)

  val invalidForJoining: Set[NodeState] = Set(Leaving, Offline)

  val validDuringDownload: Set[NodeState] =
    Set(ReadyForDownload, DownloadInProgress, DownloadCompleteAwaitingFinalSync)

  val validForDownload: Set[NodeState] = Set(PendingDownload, Ready)

  val validForRedownload: Set[NodeState] = Set(ReadyForDownload, Ready)

  val validForSnapshotCreation: Set[NodeState] = Set(Ready, Leaving)

  val validForTransactionGeneration: Set[NodeState] = Set(Ready, SnapshotCreation)

  val validForOwnConsensus: Set[NodeState] = Set(Ready, SnapshotCreation)

  val validForConsensusParticipation: Set[NodeState] = Set(Ready, SnapshotCreation)

  val validForLettingOthersDownload: Set[NodeState] = Set(Ready, SnapshotCreation, Leaving)

  val validForLettingOthersRedownload: Set[NodeState] = Set(Ready, Leaving)

  val validForCheckpointAcceptance: Set[NodeState] = Set(Ready, SnapshotCreation)

  val validForCheckpointPendingAcceptance: Set[NodeState] = validDuringDownload

  val invalidForCommunication: Set[NodeState] = Set(Offline)

  val validForHealthCheck: Set[NodeState] = readyStates ++ validDuringDownload ++ validForDownload

  // TODO: Use initial for allowing joining after leaving
  def canJoin(current: NodeState): Boolean = current == PendingDownload

  def isNotOffline(current: NodeState): Boolean = !offlineStates.contains(current)

  def isInvalidForJoining(current: NodeState): Boolean = invalidForJoining.contains(current)

  def canActAsJoiningSource(current: NodeState): Boolean = all.diff(invalidForJoining).contains(current)

  def canActAsDownloadSource(current: NodeState): Boolean = validForLettingOthersDownload.contains(current)

  def canActAsRedownloadSource(current: NodeState): Boolean = validForLettingOthersRedownload.contains(current)

  def canRunClusterCheck(current: NodeState): Boolean = validForRedownload.contains(current)

  def canCreateSnapshot(current: NodeState): Boolean = validForSnapshotCreation.contains(current)

  def canGenerateTransactions(current: NodeState): Boolean = validForTransactionGeneration.contains(current)

  def canStartOwnConsensus(current: NodeState): Boolean = validForOwnConsensus.contains(current)

  def canParticipateConsensus(current: NodeState): Boolean = validForConsensusParticipation.contains(current)

  def canAcceptCheckpoint(current: NodeState): Boolean = validForCheckpointAcceptance.contains(current)

  def canAwaitForCheckpointAcceptance(current: NodeState): Boolean =
    validForCheckpointPendingAcceptance.contains(current)

  def canUseAPI(current: NodeState): Boolean = !invalidForCommunication.contains(current)

  def canBeCheckedForHealth(current: NodeState): Boolean = validForHealthCheck.contains(current)

}
