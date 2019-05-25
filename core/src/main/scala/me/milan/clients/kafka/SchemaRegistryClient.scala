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
import me.milan.domain.Done

object SchemaRegistryClient {
  case class Schema(value: String) extends AnyVal
}

class SchemaRegistryClient[F[_]](
  config: KafkaConfig
)(
  implicit
  C: ConcurrentEffect[F]
) {
  import SchemaRegistryClient._

  private val schemaClient: CachedSchemaRegistryClient = new CachedSchemaRegistryClient(
    config.schemaRegistry.url.renderString,
    config.schemaRegistry.identityMapCapacity
  )

  def getAllSchemas: F[Set[Schema]] = C.delay(
    schemaClient.getAllSubjects.asScala.map(Schema).toSet
  )

  def deleteAllSchemas: F[Done] =
    for {
      allSubjects ← C.delay(schemaClient.getAllSubjects.asScala.toList)
      _ ← allSubjects.traverse { subject ⇒
        C.delay(schemaClient.getAllVersions(subject).asScala.toList)
          .map(_.traverse { version ⇒
            C.delay {
                schemaClient.deleteSchemaVersion(subject, version.toString)
                ()
              }
              .handleError(_ ⇒ ())
          })
      }
      _ ← allSubjects.traverse { subject ⇒
        C.delay {
            schemaClient.deleteSubject(subject)
            ()
          }
          .handleError(_ ⇒ ())
      }
    } yield Done

}
