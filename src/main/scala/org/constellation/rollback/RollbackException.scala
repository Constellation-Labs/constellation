package org.constellation.rollback

sealed trait RollbackException extends Throwable

object InvalidBalances extends RollbackException

object CannotCalculate extends RollbackException
