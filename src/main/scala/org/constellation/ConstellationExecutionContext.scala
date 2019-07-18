package org.constellation

import java.util.concurrent.Executors

import cats.effect.{ConcurrentEffect, ContextShift, IO}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.ExecutionContext

object ConstellationExecutionContext {
  val global: ExecutionContextExecutor = ExecutionContext.global
  val edge: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))
  val apiClient: ExecutionContextExecutor = edge
  val signature: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))
  val finished: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8))
}

object ConstellationContextShift {
  val global: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.global)
  val edge: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.edge)
  val apiClient: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.apiClient)
  val signature: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.signature)
  val finished: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.finished)
}

object ConstellationConcurrentEffect {
  val global: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.global)
  val edge: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.edge)
  val apiClient: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.apiClient)
  val signature: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.signature)
  val finished: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.finished)
}
