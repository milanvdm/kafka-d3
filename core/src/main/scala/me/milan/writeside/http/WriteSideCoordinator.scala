package me.milan.writeside.http

import scala.concurrent.duration._

import cats.data.EitherT
import cats.effect.{Sync, Timer}
import cats.instances.list._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.parallel._
import cats.temp.par._
import fs2.Stream
import io.circe.Decoder
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.client.middleware.{Retry, RetryPolicy}
import org.http4s.{EntityDecoder, Method, Request, Uri}

import me.milan.config.WriteSideConfig
import me.milan.domain.{Done, Error}
import me.milan.writeside.WriteSideProcessor

object WriteSide {

  def distributed[F[_], A](
    writeSideConfig: WriteSideConfig,
    httpClient: Client[F],
    writeSideProcessor: WriteSideProcessor[F, A]
  )(
    implicit
    decoder: Decoder[A],
    P: Par[F],
    T: Timer[F],
    S: Sync[F]
  ): WriteSide[F, A] = new DistributedWriteSide(writeSideConfig, httpClient, writeSideProcessor)

}

trait WriteSide[F[_], A] {

  def start: Stream[F, Done]
  def aggregateById(key: String): Stream[F, A]
  def stop: Stream[F, Done]

}

private[writeside] class DistributedWriteSide[F[_], A](
  writeSideConfig: WriteSideConfig,
  httpClient: Client[F],
  writeSideProcessor: WriteSideProcessor[F, A]
)(
  implicit
  decoder: Decoder[A],
  P: Par[F],
  T: Timer[F],
  S: Sync[F]
) extends WriteSide[F, A] {

  //TODO: Double check if this works as `writeSideProcessor.hosts` probably does not know the hosts from stopped nodes
  //TODO: Use service-discovery instead
  override def start: Stream[F, Done] = {
    val local = Stream.eval(writeSideProcessor.start)
    val distributed = DistributedWriteSide.allNodesOk(
      httpClient,
      writeSideProcessor.hosts _,
      uri ⇒ Request[F](Method.POST, uri / writeSideConfig.urlPath.value / "local" / "start")
    )

    local ++ distributed

  }

  override def aggregateById(key: String): Stream[F, A] = {
    val local = Stream.eval(writeSideProcessor.aggregateById(key))

    local.flatMap {
      case Some(aggregate) ⇒ Stream.emit(aggregate)
      case None ⇒
        DistributedWriteSide.requestNode[F, A](
          httpClient,
          () ⇒ writeSideProcessor.partitionHost(key),
          uri ⇒ Request[F](Method.GET, uri / writeSideConfig.urlPath.value / key)
        )
    }
  }

  override def stop: Stream[F, Done] = {
    val local = Stream.eval(writeSideProcessor.stop)
    val distributed = DistributedWriteSide.allNodesOk(
      httpClient,
      writeSideProcessor.hosts _,
      uri ⇒ Request[F](Method.POST, uri / writeSideConfig.urlPath.value / "local" / "stop")
    )

    local ++ distributed
  }

}

object DistributedWriteSide {

  def requestNode[F[_], A](
    httpClient: Client[F],
    host: () ⇒ F[Option[Uri]],
    toRequest: Uri ⇒ Request[F]
  )(
    implicit
    decoder: Decoder[A],
    T: Timer[F],
    S: Sync[F]
  ): Stream[F, A] = {
    val policy = RetryPolicy[F](RetryPolicy.exponentialBackoff(2.seconds, maxRetry = 3))
    val retryClient = Retry[F](policy)(httpClient)

    implicit val jsonDecoder: EntityDecoder[F, A] = jsonOf[F, A]

    val response = (for {
      foundHost ← EitherT.fromOptionF(host(), Error.KeyNotFound)
      request = toRequest(foundHost)
      response ← EitherT.liftF[F, Throwable, A](retryClient.expect(request))
    } yield response).value

    val result = S.rethrow(response)

    Stream.retry(result, 1.second, _ + 1.second, maxAttempts = 3)
  }

  def allNodesOk[F[_]](
    httpClient: Client[F],
    hosts: () ⇒ F[Set[Uri]],
    toRequest: Uri ⇒ Request[F]
  )(
    implicit
    P: Par[F],
    T: Timer[F],
    S: Sync[F]
  ): Stream[F, Done] = {
    val policy = RetryPolicy[F](RetryPolicy.exponentialBackoff(2.seconds, maxRetry = 3))
    val retryClient = Retry[F](policy)(httpClient)

    val response: F[Throwable Either Done] = for {
      foundHosts ← hosts()
      requests ← S.delay(foundHosts.toList.map(toRequest))
      responses ← requests.parTraverse(retryClient.successful)
      allSuccess = !responses.contains(false)
    } yield Either.cond[Throwable, Done](allSuccess, Done.instance, Error.HostNotFound)

    val result = S.rethrow(response)

    Stream.retry(result, 1.second, _ + 1.second, maxAttempts = 3)
  }

}
