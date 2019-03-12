package me.milan.serdes.kafka

import scala.collection.JavaConverters._

import cats.Applicative
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

import me.milan.config.KafkaConfig
import me.milan.domain.Done

class SchemaRegistryClient[F[_]](
  config: KafkaConfig
)(
  implicit
  A: Applicative[F]
) {

  private val identityMapCapacity = 100
  private val schemaClient: CachedSchemaRegistryClient = new CachedSchemaRegistryClient(
    config.schemaRegistry.url,
    identityMapCapacity
  )

  def deleteAllSchemas: F[Done] = {
    schemaClient.getAllSubjects.asScala
      .map(schemaClient.deleteSubject)

    A.pure(Done)
  }

}
