package me.milan.clients.kafka

import scala.collection.JavaConverters._

import cats.effect.ConcurrentEffect
import cats.instances.all._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

import me.milan.config.KafkaConfig

object SchemaRegistryClient {
  case class Schema(value: String) extends AnyVal

  def apply[F[_]: ConcurrentEffect](config: KafkaConfig): F[SchemaRegistryClient[F]] =
    for {
      schemaRegistryClient <- ConcurrentEffect[F].delay(
        new CachedSchemaRegistryClient(
          config.schemaRegistry.uri.renderString,
          config.schemaRegistry.identityMapCapacity
        )
      )
    } yield new SchemaRegistryClient[F](schemaRegistryClient)
}

class SchemaRegistryClient[F[_]: ConcurrentEffect](schemaRegistryClient: CachedSchemaRegistryClient) {
  import SchemaRegistryClient._

  def getAllSchemas: F[Set[Schema]] = ConcurrentEffect[F].delay(
    schemaRegistryClient.getAllSubjects.asScala.map(Schema).toSet
  )

  def deleteAllSchemas: F[Unit] =
    for {
      allSubjects <- ConcurrentEffect[F].delay(schemaRegistryClient.getAllSubjects.asScala.toList)
      _ <- allSubjects.traverse { subject =>
        ConcurrentEffect[F]
          .delay(schemaRegistryClient.getAllVersions(subject).asScala.toList)
          .map(_.traverse { version =>
            ConcurrentEffect[F]
              .delay {
                schemaRegistryClient.deleteSchemaVersion(subject, version.toString)
                ()
              }
              .handleError(_ => ())
          })
      }
      _ <- allSubjects.traverse { subject =>
        ConcurrentEffect[F]
          .delay {
            schemaRegistryClient.deleteSubject(subject)
            ()
          }
          .handleError(_ => ())
      }
    } yield ()

}
