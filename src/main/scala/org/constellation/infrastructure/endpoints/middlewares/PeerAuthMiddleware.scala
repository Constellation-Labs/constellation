package org.constellation.infrastructure.endpoints.middlewares

import java.io.ByteArrayOutputStream
import java.security.{KeyPair, MessageDigest, PrivateKey, PublicKey}

import cats.data.{Kleisli, OptionT}
import cats.effect.concurrent.Ref
import cats.effect.{Concurrent, ContextShift, Resource, Sync}
import cats.implicits._
import cats.syntax._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.IPManager.IP
import org.constellation.schema.Id
import org.http4s._
import org.http4s.client.Client
import org.http4s.dsl.io._
import pl.abankowski.httpsigner.SignatureValid
import pl.abankowski.httpsigner.http4s.{
  Http4sRequestSigner,
  Http4sRequestVerifier,
  Http4sResponseSigner,
  Http4sResponseVerifier
}
import pl.abankowski.httpsigner.signature.{Generator, Verifier}
import pl.abankowski.httpsigner.signature.generic.{GenericGenerator, GenericVerifier}

object PeerAuthMiddleware {

  def enforceKnownPeersMiddleware[F[_]: Sync](
    whitelistedPeer: IP => F[Option[Id]],
    knownPeer: IP => F[Option[Id]]
  )(http: HttpRoutes[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      val ip = req.remoteAddr.getOrElse("unknown")
      val logger = Slf4jLogger.getLogger[F]
      val unauthorizedResponse = OptionT.pure[F](Response[F](status = Unauthorized))

      val isKnownAndWhitelisted = for {
        _ <- OptionT.liftF(
          logger.debug(
            s"Middleware (Enforce known peers) ip=$ip, uri=${req.uri.path}, method=${req.method.name}, query=${req.uri.query.renderString}, headers=${req.headers}"
          )
        )
        _ <- OptionT(whitelistedPeer(ip))
        _ <- OptionT(knownPeer(ip))
        res <- http(req)
      } yield res

      isKnownAndWhitelisted.orElse(unauthorizedResponse)
    }

  def responseSignerMiddleware[F[_]: Sync](
    privateKey: PrivateKey
  )(http: HttpRoutes[F])(implicit C: ContextShift[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      http(req).flatMap { res =>
        new Http4sResponseSigner[F](getGenerator(privateKey))
          .sign(res)
          .attemptT
          .toOption
      }
    }

  def responseVerifierMiddleware[F[_]](peerId: Id)(
    client: Client[F]
  )(implicit F: Concurrent[F], C: ContextShift[F]): Client[F] =
    Client { (req: Request[F]) =>
      val verifier = new Http4sResponseVerifier[F](getVerifier(peerId.toPublicKey))

      import fs2._

      client.run(req).flatMap { response =>
        Resource.suspend {
          Ref[F].of(Vector.empty[Chunk[Byte]]).map { vec =>
            Resource.liftF {

              val copiedBody = Stream
                .eval(vec.get)
                .flatMap(v => Stream.emits(v).covary[F])
                .flatMap(c => Stream.chunk(c).covary[F])

              F.pure {
                response.copy(
                  body = response.body
                    .observe(_.chunks.flatMap(s => Stream.eval_(vec.update(_ :+ s))))
                )
              }.flatMap(verifier.verify).flatMap {
                case SignatureValid => F.pure(response.withBodyStream(copiedBody))
                case _ =>
                  F.pure(Response[F](status = Unauthorized))
              }
            }
          }
        }
      }
    }

  def requestSignerMiddleware[F[_]](client: Client[F], privateKey: PrivateKey, selfIp: String)(
    implicit F: Concurrent[F],
    C: ContextShift[F]
  ): Client[F] =
    Client { req: Request[F] =>
      import fs2._

      val logger = Slf4jLogger.getLogger[F]
      val signer = new Http4sRequestSigner(getGenerator(privateKey))

      Resource.suspend {
        Ref[F].of(Vector.empty[Chunk[Byte]]).map { vec =>
          Resource.liftF {

            val copiedBody = Stream
              .eval(vec.get)
              .flatMap(v => Stream.emits(v).covary[F])
              .flatMap(c => Stream.chunk(c).covary[F])

            val newReq = req.withBodyStream(req.body.observe(_.chunks.flatMap(s => Stream.eval_(vec.update(_ :+ s)))))

            logger.debug(
              s"Middleware (Signer) ip=$selfIp, uri=${req.uri.path}, method=${req.method.name}, query=${req.uri.query.renderString}, headers=${req.headers}}"
            ) >> signer
              .sign(newReq)
              .map { r =>
                req.withBodyStream(copiedBody).withHeaders(r.headers)
              }
          }
        }
      } >>= client.run
    }

  def requestVerifierMiddleware[F[_]](
    knownPeer: IP => F[Option[Id]]
  )(http: HttpRoutes[F])(implicit C: ContextShift[F], F: Concurrent[F]): HttpRoutes[F] =
    Kleisli { req: Request[F] =>
      val logger = Slf4jLogger.getLogger[F]
      val ip = req.remoteAddr.getOrElse("unknown")
      val unauthorizedResponse: OptionT[F, Response[F]] = OptionT.pure[F](Response[F](status = Unauthorized))

      val verify: OptionT[F, Response[F]] = for {
        _ <- OptionT.liftF(
          logger.debug(
            s"Middleware (Verifier) ip=$ip, uri=${req.uri.path}, method=${req.method.name}, query=${req.uri.query.renderString}, headers=${req.headers}"
          )
        )

        tuple <- Ref[F]
          .of(Vector.empty[Chunk[Byte]])
          .map { vec =>
            val newBody = Stream
              .eval(vec.get)
              .flatMap(v => Stream.emits(v).covary[F])
              .flatMap(c => Stream.chunk(c).covary[F])
            val newReq = req.withBodyStream(req.body.observe(_.chunks.flatMap(s => Stream.eval_(vec.update(_ :+ s)))))
            (newBody, newReq)
          }
          .attemptT
          .toOption

        id <- knownPeer(ip).attemptT.toOption.flatMap(_.toOptionT)

        crypto = getVerifier(id.toPublicKey)
        verifier = new Http4sRequestVerifier[F](crypto)

        verifierResult <- verifier
          .verify(tuple._2)
          .attemptT
          .toOption

        response <- verifierResult match {
          case SignatureValid => http(req.withBodyStream(tuple._1))
          case _              => unauthorizedResponse
        }
      } yield response

      verify.orElse(unauthorizedResponse)
    }

  private def getVerifier(publicKey: PublicKey): Verifier =
    GenericVerifier(KeyUtils.DefaultSignFunc, KeyUtils.provider, publicKey)

  private def getGenerator(privateKey: PrivateKey): Generator =
    GenericGenerator(KeyUtils.DefaultSignFunc, KeyUtils.provider, privateKey)
}
