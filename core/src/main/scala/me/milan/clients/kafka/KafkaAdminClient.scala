package me.milan.clients.kafka

import java.util.Properties

import scala.collection.JavaConverters._

import cats.effect.ConcurrentEffect
import cats.instances.finiteDuration._
import cats.instances.set._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.show._
import org.apache.kafka.clients.admin.AdminClientConfig._
import org.apache.kafka.clients.admin.{ AdminClient, ConsumerGroupDescription, MemberDescription, NewTopic }
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.http4s.Uri

import me.milan.config.KafkaConfig
import me.milan.config.KafkaConfig.BootstrapServer._
import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.{ Done, Error, Topic }
import me.milan.pubsub.kafka.KConsumer.ConsumerGroupId

class KafkaAdminClient[F[_]](
  config: KafkaConfig
)(
  implicit
  E: ConcurrentEffect[F]
) {

  private val props = new Properties()
  props.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.show)

  private val adminClient: AdminClient = AdminClient.create(props)

  def createTopics: F[Done] = createTopics(config.topics.toSet)

  def createTopics(topicConfigs: Set[TopicConfig]): F[Done] = {

    val newTopics = topicConfigs.map { topicConfig ⇒
      new NewTopic(
        topicConfig.name.value,
        topicConfig.partitions.value,
        topicConfig.replicationFactor.value.toShort
      ).configs(
        Map(
          "delete.retention.ms" → topicConfig.retention.show,
          "retention.ms" → topicConfig.retention.show
        ).asJava
      )
    }

    E.async[Done] { cb ⇒
        adminClient
          .createTopics(newTopics.asJavaCollection)
          .all()
          .whenComplete { (_, throwable) ⇒
            cb(Option(throwable).toLeft(Done.instance))
          }
        ()
      }
      .handleError {
        case _: org.apache.kafka.common.errors.TopicExistsException ⇒ Done
      }
      .adaptError {
        case e ⇒ Error.System(e)
      }
  }

  def createTopic(topicConfig: TopicConfig): F[Done] = createTopics(Set(topicConfig))

  def getTopics(ignoreSystemTopics: Boolean = true): F[Set[Topic]] =
    E.async[Set[Topic]] { cb ⇒
        adminClient.listTopics().names().whenComplete { (topics, throwable) ⇒
          cb(
            Option(throwable)
              .toLeft(
                topics.asScala.toSet
                  .filterNot(_.startsWith("_") && ignoreSystemTopics)
                  .map(Topic)
              )
          )
        }
        ()
      }
      .adaptError {
        case e ⇒ Error.System(e)
      }

  def deleteTopic(topic: Topic): F[Done] = deleteTopics(Set(topic))

  def deleteAllTopics: F[Done] =
    for {
      topics ← getTopics()
      _ ← deleteTopics(topics)
    } yield Done

  private def deleteTopics(topics: Set[Topic]): F[Done] =
    E.async[Done] { cb ⇒
        adminClient
          .deleteTopics(topics.map(_.value).asJavaCollection)
          .all
          .whenComplete { (_, throwable) ⇒
            cb(Option(throwable).toLeft(Done))
          }
        ()
      }
      .recover {
        case _: UnknownTopicOrPartitionException ⇒ Done
      }
      .adaptError {
        case e ⇒ Error.System(e)
      }

  def consumerGroupHosts(consumerGroupId: ConsumerGroupId): F[Set[Uri]] =
    consumerGroupMembers(consumerGroupId)
      .map(_.map(_.host).map(Uri.unsafeFromString))

  def consumerGroupMembers(consumerGroupId: ConsumerGroupId): F[Set[MemberDescription]] =
    E.async[Option[ConsumerGroupDescription]] { cb ⇒
        adminClient
          .describeConsumerGroups(Seq(consumerGroupId.value).asJavaCollection)
          .all
          .whenComplete { (consumerGroups, throwable) ⇒
            cb(Option(throwable).toLeft(consumerGroups.asScala.values.headOption))
          }
        ()
      }
      .map(_.map(_.members.asScala.toSet))
      .map(_.getOrElse(Set.empty))

//  // Based on https://github.com/apache/kafka/blob/2.1.1/core/src/main/scala/kafka/admin/ConsumerGroupCommand.scala#L300
//  def resetConsumerGroup(consumerGroupId: ConsumerGroupId, timestamp: Long) = {
//
//    val consumer = new KConsumer(config, consumerGroupId).consumer
//
//    val partitionsToReset = E.async[List[TopicPartition]] { cb ⇒
//      adminClient
//        .listConsumerGroupOffsets(
//          consumerGroupId.value,
//          (new ListConsumerGroupOffsetsOptions).timeoutMs(1.second.toMillis.toInt)
//        )
//        .partitionsToOffsetAndMetadata
//        .whenComplete { (partitionsToReset, throwable) ⇒
//          cb(Option(throwable).toLeft(partitionsToReset.asScala.values).getOrElse(List.empty))
//        }
//      ()
//    }
//
//    partitionsToReset.map { partitions =>
//      consumer.assign(partitions.asJava)
//
//      val offsetsForTimes = consumer.offsetsForTimes(partitions.map(_ -> Long.box(timestamp)).toMap.asJava).asScala
//      val endOffsets = consumer.endOffsets(offsetsForTimes.filter(_._2 == null).keySet.asJava)
//
//    }
//
//
//
//
//
//    val successfulLogTimestampOffsets = successfulOffsetsForTimes.map {
//      case (topicPartition, offsetAndTimestamp) => topicPartition -> LogOffsetResult.LogOffset(offsetAndTimestamp.offset)
//    }.toMap
//
//    successfulLogTimestampOffsets ++ getLogEndOffsets(unsuccessfulOffsetsForTimes.keySet.toSeq)
//
//
//
//    val offsets = getConsumer.endOffsets(topicPartitions.asJava)
//    topicPartitions.map { topicPartition =>
//      Option(offsets.get(topicPartition)) match {
//        case Some(logEndOffset) => topicPartition -> LogOffsetResult.LogOffset(logEndOffset)
//        case _ => topicPartition -> LogOffsetResult.Unknown
//      }
//    }.toMap
//
//
//
//
//    partitionsToReset.map { topicPartition =>
//      val logTimestampOffset = logTimestampOffsets.get(topicPartition)
//      logTimestampOffset match {
//        case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
//        case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
//      }
//    }.toMap
//
//  }
//
//
//  def resetOffsets(): Map[TopicPartition, OffsetAndMetadata] = {
//    val groupId = opts.options.valueOf(opts.groupOpt)
//    val consumerGroups = adminClient.describeConsumerGroups(
//      util.Arrays.asList(groupId),
//      withTimeoutMs(new DescribeConsumerGroupsOptions)
//    ).describedGroups()
//
//    val group = consumerGroups.get(groupId).get
//    group.state.toString match {
//      case "Empty" | "Dead" =>
//        val partitionsToReset = getPartitionsToReset(groupId)
//        val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)
//
//        // Dry-run is the default behavior if --execute is not specified
//        val dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt)
//        if (!dryRun)
//          getConsumer.commitSync(preparedOffsets.asJava)
//        preparedOffsets
//      case currentState =>
//        printError(s"Assignments can only be reset if the group '$groupId' is inactive, but the current state is $currentState.")
//        Map.empty
//    }
//  }
//
//
//
//
//  private def getLogTimestampOffsets(topicPartitions: Seq[TopicPartition], timestamp: java.lang.Long): Map[TopicPartition, LogOffsetResult] = {
//    val consumer = getConsumer
//    consumer.assign(topicPartitions.asJava)
//
//    val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
//      consumer.offsetsForTimes(topicPartitions.map(_ -> timestamp).toMap.asJava).asScala.partition(_._2 != null)
//
//    val successfulLogTimestampOffsets = successfulOffsetsForTimes.map {
//      case (topicPartition, offsetAndTimestamp) => topicPartition -> LogOffsetResult.LogOffset(offsetAndTimestamp.offset)
//    }.toMap
//
//    successfulLogTimestampOffsets ++ getLogEndOffsets(unsuccessfulOffsetsForTimes.keySet.toSeq)
//
//
//    private def getLogEndOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
//      val offsets = getConsumer.endOffsets(topicPartitions.asJava)
//      topicPartitions.map { topicPartition =>
//        Option(offsets.get(topicPartition)) match {
//          case Some(logEndOffset) => topicPartition -> LogOffsetResult.LogOffset(logEndOffset)
//          case _ => topicPartition -> LogOffsetResult.Unknown
//        }
//      }.toMap
//    }
//  }
//
//  private def checkOffsetsRange(requestedOffsets: Map[TopicPartition, Long]) = {
//    val logStartOffsets = getLogStartOffsets(requestedOffsets.keySet.toSeq)
//    val logEndOffsets = getLogEndOffsets(requestedOffsets.keySet.toSeq)
//    requestedOffsets.map { case (topicPartition, offset) => (topicPartition,
//      logEndOffsets.get(topicPartition) match {
//        case Some(LogOffsetResult.LogOffset(endOffset)) if offset > endOffset =>
//          warn(s"New offset ($offset) is higher than latest offset for topic partition $topicPartition. Value will be set to $endOffset")
//          endOffset
//
//        case Some(_) => logStartOffsets.get(topicPartition) match {
//          case Some(LogOffsetResult.LogOffset(startOffset)) if offset < startOffset =>
//            warn(s"New offset ($offset) is lower than earliest offset for topic partition $topicPartition. Value will be set to $startOffset")
//            startOffset
//
//          case _ => offset
//        }
//
//        case None => // the control should not reach here
//          throw new IllegalStateException(s"Unexpected non-existing offset value for topic partition $topicPartition")
//      })
//    }
//  }
//
//  def exportOffsetsToReset(assignmentsToReset: Map[TopicPartition, OffsetAndMetadata]): String = {
//    val rows = assignmentsToReset.map { case (k,v) => s"${k.topic},${k.partition},${v.offset}" }(collection.breakOut): List[String]
//    rows.foldRight("")(_ + "\n" + _)
//  }

}
