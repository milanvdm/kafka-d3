package me.milan.store

import cats.data.EitherT
import cats.effect.{Concurrent, Sync, Timer}
import fs2.Stream
import scala.concurrent.duration._

import cats.Parallel
import io.circe.Decoder
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.http4s.{EntityDecoder, Method, Request, Uri}
import org.http4s.circe.jsonOf
import org.http4s.client.Client
import cats.syntax.flatMap._
import cats.syntax.functor._
import org.http4s.client.middleware.{Retry, RetryPolicy}
import cats.syntax.parallel._
import cats.syntax.list._
import cats.instances.list._
import org.http4s.circe._
import cats.syntax.applicativeError._
import cats.syntax.monadError._

import me.milan.config.AggregateStoreConfig
import me.milan.discovery.Registry
import me.milan.discovery.Registry.RegistryGroup
import me.milan.domain.Error
import me.milan.serdes.AvroSerde
import me.milan.store.kafka.StoreName

object AggregateStore {

  def kafka[F[_]: Sync, A: AvroSerde](
    aggregateStoreConfig: AggregateStoreConfig,
                                       httpClient: Client[F],
    registry: Registry[F],
    kafkaStream: KafkaStreams,
    storeName: StoreName,
                                       registryGroup: RegistryGroup
  ): AggregateStore[F, A] = KafkaAggregateStore(registry, kafkaStream, storeName)

}

trait AggregateStore[F[_], A] {

  def byId(id: String): F[Option[A]]

}

case class KafkaAggregateStore[F[_]: Sync: Parallel: Timer, A: AvroSerde: Decoder](
                                                                          aggregateStoreConfig: AggregateStoreConfig,
                                                          httpClient: Client[F],
                                                          registry: Registry[F],
  stream: KafkaStreams,
  storeName: StoreName,
                                                          registryGroup: RegistryGroup
) extends AggregateStore[F, A] {

  override def byId(id: String): F[Option[A]] = Sync[F].delay {
    val encodedAggregate = Option(
      stream
        .store(storeName.value, QueryableStoreTypes.keyValueStore[String, GenericRecord])
        .get(id)
    )
    encodedAggregate.map(AvroSerde[A].decode)
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
