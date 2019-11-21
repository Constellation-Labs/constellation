package org.constellation.rollback

sealed trait RollbackException extends Throwable

object InvalidBalances extends RollbackException

object CannotCalculate extends RollbackException

case class CannotLoadSnapshotsFiles(path: String) extends RollbackException

case class CannotLoadGenesisObservationFile(path: String) extends RollbackException

case class CannotLoadSnapshotInfoFile(path: String) extends RollbackException

object CannotWriteToDisk extends RollbackException
