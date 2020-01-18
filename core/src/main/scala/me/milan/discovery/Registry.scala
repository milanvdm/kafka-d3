package me.milan.discovery

import java.time.OffsetDateTime

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{ FiniteDuration, _ }

import cats.effect.IO.Async
import cats.effect.{ Async, Sync, Timer }
import com.orbitz.consul.Consul
import com.orbitz.consul.async.ConsulResponseCallback
import com.orbitz.consul.model.ConsulResponse
import com.orbitz.consul.model.agent.{ ImmutableRegistration, Registration }
import com.orbitz.consul.option.QueryOptions
import fs2.Stream
import org.http4s.Uri
import cats.syntax.either._
import com.orbitz.consul.model.health.ServiceHealth
import cats.syntax.functor._

import me.milan.domain.Key

object Registry {

  case class RegistryGroup(
    id: Key,
    name: String
  )

//  def consul
//
//  def dummy

}

trait Registry[F[_]] {
  import Registry._

  def register(serviceGroup: RegistryGroup): Stream[F, Unit]

  def getHosts(serviceGroup: RegistryGroup): F[List[Uri]]

}

private[discovery] class ConsulRegistry[F[_]: Async: Timer](
  timeToLive: FiniteDuration,
  consulClient: Consul
) extends Registry[F] {
  import Registry._

  private val agentClient = consulClient.agentClient
  private val healthClient = consulClient.healthClient

  override def register(serviceGroup: RegistryGroup): Stream[F, Unit] = {
    val service = ImmutableRegistration.builder
      .id(serviceGroup.id.value)
      .name(serviceGroup.name)
      .port(8080) //TODO get port from config
      .check(Registration.RegCheck.ttl(timeToLive.toSeconds))
      .build()

    for {
      _ <- Stream.eval(Sync[F].delay(agentClient.register(service)))
      repeat <- (checkIn(serviceGroup) ++ Stream.sleep_(timeToLive / 2)).repeat
    } yield repeat
  }

  override def getHosts(serviceGroup: RegistryGroup): F[List[Uri]] =
    Async[F]
      .async[List[ServiceHealth]] { cb =>
        healthClient
          .getHealthyServiceInstances(
            serviceGroup.name,
            QueryOptions.BLANK,
            new ConsulResponseCallback[java.util.List[ServiceHealth]] {
              override def onComplete(consulResponse: ConsulResponse[java.util.List[ServiceHealth]]): Unit =
                cb(consulResponse.getResponse.asScala.toList.asRight)
              override def onFailure(throwable: Throwable): Unit = cb(throwable.asLeft)
            }
          )
      }
      .map(_.map(_.getNode.getAddress))
      .map(_.map(Uri.fromString))
      .map(_.collect { case Right(uri) => uri })

  private def checkIn(serviceGroup: RegistryGroup): Stream[F, Unit] = Stream.retry(
    Sync[F].delay(agentClient.pass(serviceGroup.id.value)),
    100.millis,
    x => x,
    maxAttempts = 5
  )
}

private[discovery] class DummyRegistry[F[_]: Sync: Timer](
  serviceId: Key,
  timeToLive: FiniteDuration
) extends Registry[F] {
  import Registry._

  private case class Registration(
    uri: String,
    timestamp: OffsetDateTime
  )

  private val registry = new TrieMap[RegistryGroup, Registration]

  override def register(serviceGroup: RegistryGroup): Stream[F, Unit] =
    for {
      _ <- Stream.eval(Sync[F].delay(agentClient.register(service)))
      repeat <- (checkIn ++ Stream.sleep_(timeToLive / 2)).repeat
    } yield repeat

  override def getHosts(serviceGroup: RegistryGroup): F[List[Uri]] = ???

  private def checkIn: Stream[F, Unit] = Stream.eval()
}
