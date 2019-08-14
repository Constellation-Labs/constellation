package org.constellation

import java.util.concurrent.Executors

import cats.effect.{ConcurrentEffect, ContextShift, IO}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.ExecutionContext

object ConstellationExecutionContext {

  // By default number of available cpus use for CPU-intensive work
  val bounded = ExecutionContext.fromExecutor(Executors.newWorkStealingPool())
  //  an unbounded thread pool for executing blocking I/O calls
  val unbounded = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  //  a bounded thread pool for non-blocking I/O callbacks
  val callbacks = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

//  val boundedShift: ContextShift[IO] = IO.contextShift(bounded)
//  val unboundedShift: ContextShift[IO] = IO.contextShift(unbounded)
//  val callbacksShift: ContextShift[IO] = IO.contextShift(callbacks)
//  val boundedEffect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(boundedShift)
//  val unboundedEffect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(unboundedShift)
//  val callbacksEffect: ConcurrentEffect[IO] = IO.ioConcurrentEffect(callbacksShift)

  val global: ExecutionContextExecutor = unbounded
  val edge: ExecutionContextExecutor = bounded
  val apiClient: ExecutionContextExecutor = callbacks
  val signature: ExecutionContextExecutor = bounded
  val finished: ExecutionContextExecutor = bounded
}

object ConstellationContextShift {
  val global: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.global)
  val apiClient: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.apiClient)

  val edge: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.edge)
  val signature: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.signature)
  val finished: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.finished)
}

object ConstellationConcurrentEffect {
  val global: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.global)
  val apiClient: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.apiClient)

  val edge: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.edge)
  val signature: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.signature)
  val finished: ConcurrentEffect[IO] = IO.ioConcurrentEffect(ConstellationContextShift.finished)
}
