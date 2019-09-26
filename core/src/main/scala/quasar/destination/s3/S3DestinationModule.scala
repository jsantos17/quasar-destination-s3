/*
 * Copyright 2014–2019 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.s3

import slamdata.Predef._

import quasar.api.destination.DestinationError
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{Destination, DestinationType}
import quasar.concurrent.NamedDaemonThreadFactory
import quasar.connector.{DestinationModule, MonadResourceErr}
import quasar.destination.s3.impl.DefaultUpload

import java.lang.Runtime
import java.util.concurrent.Executors

import argonaut.{Argonaut, Json}, Argonaut._
import software.amazon.awssdk.core.client.config.{
  ClientAsyncConfiguration,
  SdkAdvancedAsyncClientOption
}
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.regions.{Region => AwsRegion}
import software.amazon.awssdk.services.s3.model.{
  HeadBucketRequest,
  NoSuchBucketException,
  S3Exception
}
import cats.Eq
import cats.data.EitherT
import cats.effect.{Async, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.instances.int._
import cats.syntax.applicativeError._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.flatMap._
import eu.timepit.refined.auto._
import monix.catnap.syntax._
import scalaz.NonEmptyList

object S3DestinationModule extends DestinationModule {
  // Minimum 10MiB multipart uploads
  private val PartSize = 10 * 1024 * 1024
  private val Redacted = "<REDACTED>"
  private val RedactedCreds = S3Credentials(AccessKey(Redacted), SecretKey(Redacted), Region(Redacted))
  private val AsyncPoolPrefix = "s3-async-dest"

  def destinationType = DestinationType("s3", 1L)

  def sanitizeDestinationConfig(config: Json) = config.as[S3Config].result match {
    case Left(_) => Json.jEmptyObject // don't expose credentials, even if we fail to decode the configuration.
    case Right(cfg) => cfg.copy(credentials = RedactedCreds).asJson
  }

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)
      : Resource[F, Either[InitializationError[Json], Destination[F]]] = {

    val configOrError = config.as[S3Config].toEither.leftMap {
      case (err, _) =>
        DestinationError.malformedConfiguration((destinationType, config, err))
    }

    val sanitizedConfig = sanitizeDestinationConfig(config)

    (for {
      cfg <- EitherT(Resource.pure[F, Either[InitializationError[Json], S3Config]](configOrError))
      client <- EitherT(mkClient(cfg).map(_.asRight[InitializationError[Json]]))
      upload = DefaultUpload(client, PartSize)
      _ <- EitherT(Resource.liftF(isLive(client, sanitizedConfig, cfg.bucket)))
    } yield (S3Destination(cfg.bucket, upload): Destination[F])).value
  }

  private def isLive[F[_]: Async](client: S3AsyncClient, originalConfig: Json, bucket: Bucket)
      : F[Either[InitializationError[Json], Unit]] =
    Async[F].delay(client.headBucket(HeadBucketRequest.builder.bucket(bucket.value).build))
      .futureLift.as(().asRight[InitializationError[Json]]) recover {
        case (_: NoSuchBucketException) =>
          DestinationError.invalidConfiguration((
            destinationType,
            originalConfig,
            NonEmptyList("Bucket does not exist"))).asLeft
        // eq syntax is buggy in this case. Don't use it. It will lock-up the thread handling the request
        // in slamdata-backend
        case (e: S3Exception) if Eq[Int].eqv(403, e.statusCode) =>
          DestinationError.accessDenied((
            destinationType,
            originalConfig,
            "Access denied")).asLeft
      }

  private def mkClient[F[_]: Sync](cfg: S3Config): Resource[F, S3AsyncClient] = {
    val allocateExecutor =
      Sync[F].delay(Runtime.getRuntime.availableProcessors).flatMap(cores =>
        Sync[F].delay(Executors.newFixedThreadPool(cores, NamedDaemonThreadFactory(AsyncPoolPrefix))))

    val executorService =
      Resource.make(allocateExecutor)(es => Sync[F].delay(es.shutdown()))

    executorService.flatMap(es =>
      Resource.fromAutoCloseable(
        Sync[F].delay(
          S3AsyncClient
            .builder
            .asyncConfiguration(
              ClientAsyncConfiguration
                .builder
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, es)
                .build)
            .credentialsProvider(
              StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                  cfg.credentials.accessKey.value,
                  cfg.credentials.secretKey.value)))
            .region(AwsRegion.of(cfg.credentials.region.value))
            .build)))
  }
}
