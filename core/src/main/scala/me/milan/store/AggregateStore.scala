package me.milan.store

import scala.concurrent.duration._

import cats.Parallel
import cats.data.OptionT
import cats.effect.{ Concurrent, Sync, Timer }
import cats.instances.list._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.parallel._
import io.circe.Decoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.http4s.circe.jsonOf
import org.http4s.client.Client
import org.http4s.client.middleware.{ Retry, RetryPolicy }
import org.http4s.{ EntityDecoder, Method, Request }

import me.milan.config.AggregateStoreConfig
import me.milan.discovery.Registry
import me.milan.discovery.Registry.RegistryGroup
import me.milan.serdes.AvroSerde
import me.milan.store.kafka.StoreName

object AggregateStore {

  def kafka[F[_]: Concurrent: Parallel: Timer, A >: Null: AvroSerde: Decoder](
    aggregateStoreConfig: AggregateStoreConfig,
    httpClient: Client[F],
    registry: Registry[F],
    kafkaStream: KafkaStreams,
    storeName: StoreName,
    registryGroup: RegistryGroup
  ): AggregateStore[F, A] =
    KafkaAggregateStore(aggregateStoreConfig, httpClient, registry, kafkaStream, storeName, registryGroup)

}

trait AggregateStore[F[_], A] {

  def byId(id: String): F[Option[A]]

}

case class KafkaAggregateStore[F[_]: Concurrent: Parallel: Timer, A >: Null: AvroSerde: Decoder](
  aggregateStoreConfig: AggregateStoreConfig,
  httpClient: Client[F],
  registry: Registry[F],
  stream: KafkaStreams,
  storeName: StoreName,
  registryGroup: RegistryGroup
) extends AggregateStore[F, A] {

  override def byId(id: String): F[Option[A]] = {
    val encodedAggregate = Sync[F].delay {
      Option(
        stream
          .store(storeName.value, QueryableStoreTypes.keyValueStore[String, GenericRecord])
          .get(id)
      )
    }

    OptionT(encodedAggregate)
      .map(AvroSerde[A].decode)
      .orElseF(requestAllNodesById(id))
      .value
  }

  private def requestAllNodesById(id: String): F[Option[A]] = {
    val policy = RetryPolicy[F](RetryPolicy.exponentialBackoff(500.millis, maxRetry = 3))
    val retryClient = Retry[F](policy)(httpClient)

    implicit val jsonDecoder: EntityDecoder[F, A] = jsonOf[F, A]

    for {
      hosts <- registry.getHosts(registryGroup)
      requests = hosts.map(host => Request[F](Method.GET, host / aggregateStoreConfig.urlPath.value / id))
      response <- requests.parTraverse(retryClient.expect(_).attempt).map(_.collectFirst { case Right(r) => r })
    } yield response
  }

}
