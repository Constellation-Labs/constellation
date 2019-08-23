package org.constellation.rollback

sealed trait RollbackException

object InvalidBalances extends RollbackException

object CannotCalculate extends RollbackException

object CannotLoadSnapshotsFiles extends RollbackException
object CannotLoadGenesisObservationFile extends RollbackException
object CannotLoadSnapshotInfoFile extends RollbackException

object CannotWriteToDisk extends RollbackException
