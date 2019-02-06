package org.constellation.util

import cats.data.{Validated, ValidatedNel}

import scala.concurrent.{ExecutionContext, Future}

object Validation {

//  def toValidatedNel[A](future: Future[A])(implicit ec: ExecutionContext): Future[ValidatedNel[Throwable, A]] = {
//    future.map(Validated.valid).recover { case e =>
//      Validated.invalidNel(e)
//    }
//  }

  implicit class EnrichedFuture[A](future: Future[A]) {
    def toValidatedNel(implicit ec: ExecutionContext): Future[ValidatedNel[Throwable, A]] = {
      future.map(Validated.valid).recover {
        case e =>
          Validated.invalidNel(e)
      }
    }
  }

}
