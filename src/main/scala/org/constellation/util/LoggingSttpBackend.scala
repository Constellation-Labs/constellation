package org.constellation.util

import com.softwaremill.sttp.{MonadError, Request, Response, SttpBackend}
import com.typesafe.scalalogging.{LoggerTakingImplicit, StrictLogging}

class LoggingSttpBackend[R[_], S](delegate: SttpBackend[R, S])(
  implicit val apiLogger: LoggerTakingImplicit[HostPort],
  hp: HostPort
) extends SttpBackend[R, S]
    with StrictLogging {

  override def send[T](request: Request[T, S]): R[Response[T]] = {
    responseMonad.map(responseMonad.handleError(delegate.send(request)) {
      case e: Exception =>
        apiLogger.error(s"Exception when sending request: $request", e)
        responseMonad.error(e)
    }) { response =>
      if (response.isSuccess) {
        logger.whenTraceEnabled {
          apiLogger.trace(s"For request: $request got response: $response")
        }
        logger.whenDebugEnabled {

          val respText = {
            val r = response
            s"Response(${r.code},${r.statusText})"
          }

          val reqText = {
            val r = request
            s"Request(${r.method},${r.uri},${r.headers})"
          }
          apiLogger.debug(s"For request: $reqText got response: $respText")
        }
      } else {
        apiLogger.warn(s"For request: $request got response: $response")
      }
      response
    }
  }

  override def close(): Unit = delegate.close()

  override def responseMonad: MonadError[R] = delegate.responseMonad
}
