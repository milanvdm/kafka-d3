package me.milan.clients.kafka

import java.util.Properties

import scala.collection.JavaConverters._

import cats.effect.ConcurrentEffect
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import org.apache.kafka.clients.admin.AdminClientConfig._
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException

import me.milan.config.KafkaConfig
import me.milan.config.KafkaConfig.TopicConfig
import me.milan.domain.{Done, Error, Topic}

class KafkaAdminClient[F[_]](
  config: KafkaConfig
)(
  implicit
  E: ConcurrentEffect[F]
) {

  private val props = new Properties()
  props.setProperty(BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers.map(_.value).mkString(","))

  private val adminClient: AdminClient = AdminClient.create(props)

  def createTopics: F[Done] = createTopics(config.topics.toSet)

  def createTopics(topicConfigs: Set[TopicConfig]): F[Done] = {

    val topicDefaultConfig: Map[String, String] = Map(
      "delete.retention.ms" → Long.MaxValue.toString,
      "retention.ms" → Long.MaxValue.toString
    )

    val newTopics = topicConfigs.map { topicConfig ⇒
      new NewTopic(
        topicConfig.name.value,
        topicConfig.partitions.value,
        topicConfig.replicationFactor.value.toShort
      ).configs(topicDefaultConfig.asJava)
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

  // Based on https://github.com/apache/kafka/blob/2.1.1/core/src/main/scala/kafka/admin/ConsumerGroupCommand.scala#L300
  def offsetsToReset(consumerGroupId: ConsumerGroupId): F[Map[TopicPartition, OffsetAndMetadata]] = {
    val consumerGroups = adminClient
      .describeConsumerGroups(List(consumerGroupId.value).asJava)
      .describedGroups()
      .asScala

    val group = consumerGroups.get(consumerGroupId.value).get.get
    group.state.toString match {
      case "Empty" | "Dead" =>
        val partitionsToReset = getPartitionsToReset(groupId)
        val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)

        getConsumer.commitSync(preparedOffsets.asJava)
        preparedOffsets
      case currentState =>
        printError(s"Assignments can only be reset if the group '$groupId' is inactive, but the current state is $currentState.")

    }
  }
//
//  private def getPartitionsToReset(groupId: String): Seq[TopicPartition] = {
//    if (opts.options.has(opts.allTopicsOpt)) {
//      val allTopicPartitions = getCommittedOffsets(groupId).keySet().asScala.toSeq
//      allTopicPartitions
//    } else if (opts.options.has(opts.topicOpt)) {
//      val topics = opts.options.valuesOf(opts.topicOpt).asScala
//      parseTopicPartitionsToReset(topics)
//    } else {
//      if (opts.options.has(opts.resetFromFileOpt))
//        Nil
//      else
//        CommandLineUtils.printUsageAndDie(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.")
//    }
//  }
//
//  private def prepareOffsetsToReset(groupId: String, partitionsToReset: Seq[TopicPartition]): Map[TopicPartition, OffsetAndMetadata] = {
//    if (opts.options.has(opts.resetToOffsetOpt)) {
//      val offset = opts.options.valueOf(opts.resetToOffsetOpt)
//      checkOffsetsRange(partitionsToReset.map((_, offset)).toMap).map {
//        case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
//      }
//    } else if (opts.options.has(opts.resetToEarliestOpt)) {
//      val logStartOffsets = getLogStartOffsets(partitionsToReset)
//      partitionsToReset.map { topicPartition =>
//        logStartOffsets.get(topicPartition) match {
//          case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
//          case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting starting offset of topic partition: $topicPartition")
//        }
//      }.toMap
//    } else if (opts.options.has(opts.resetToLatestOpt)) {
//      val logEndOffsets = getLogEndOffsets(partitionsToReset)
//      partitionsToReset.map { topicPartition =>
//        logEndOffsets.get(topicPartition) match {
//          case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
//          case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
//        }
//      }.toMap
//    } else if (opts.options.has(opts.resetShiftByOpt)) {
//      val currentCommittedOffsets = getCommittedOffsets(groupId)
//      val requestedOffsets = partitionsToReset.map { topicPartition =>
//        val shiftBy = opts.options.valueOf(opts.resetShiftByOpt)
//        val currentOffset = currentCommittedOffsets.asScala.getOrElse(topicPartition,
//          throw new IllegalArgumentException(s"Cannot shift offset for partition $topicPartition since there is no current committed offset")).offset
//        (topicPartition, currentOffset + shiftBy)
//      }.toMap
//      checkOffsetsRange(requestedOffsets).map {
//        case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
//      }
//    } else if (opts.options.has(opts.resetToDatetimeOpt)) {
//      val timestamp = convertTimestamp(opts.options.valueOf(opts.resetToDatetimeOpt))
//      val logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp)
//      partitionsToReset.map { topicPartition =>
//        val logTimestampOffset = logTimestampOffsets.get(topicPartition)
//        logTimestampOffset match {
//          case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
//          case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
//        }
//      }.toMap
//    } else if (opts.options.has(opts.resetByDurationOpt)) {
//      val duration = opts.options.valueOf(opts.resetByDurationOpt)
//      val durationParsed = DatatypeFactory.newInstance().newDuration(duration)
//      val now = new Date()
//      durationParsed.negate().addTo(now)
//      val timestamp = now.getTime
//      val logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp)
//      partitionsToReset.map { topicPartition =>
//        val logTimestampOffset = logTimestampOffsets.get(topicPartition)
//        logTimestampOffset match {
//          case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
//          case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
//        }
//      }.toMap
//    } else if (opts.options.has(opts.resetFromFileOpt)) {
//      val resetPlanPath = opts.options.valueOf(opts.resetFromFileOpt)
//      val resetPlanCsv = Utils.readFileAsString(resetPlanPath)
//      val resetPlan = parseResetPlan(resetPlanCsv)
//      val requestedOffsets = resetPlan.keySet.map { topicPartition =>
//        (topicPartition, resetPlan(topicPartition).offset)
//      }.toMap
//      checkOffsetsRange(requestedOffsets).map {
//        case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
//      }
//    } else if (opts.options.has(opts.resetToCurrentOpt)) {
//      val currentCommittedOffsets = getCommittedOffsets(groupId)
//      val (partitionsToResetWithCommittedOffset, partitionsToResetWithoutCommittedOffset) =
//        partitionsToReset.partition(currentCommittedOffsets.keySet.contains(_))
//
//      val preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.map { topicPartition =>
//        (topicPartition, new OffsetAndMetadata(currentCommittedOffsets.get(topicPartition) match {
//          case offset if offset != null => offset.offset
//          case _ => throw new IllegalStateException(s"Expected a valid current offset for topic partition: $topicPartition")
//        }))
//      }.toMap
//
//      val preparedOffsetsForPartitionsWithoutCommittedOffset = getLogEndOffsets(partitionsToResetWithoutCommittedOffset).map {
//        case (topicPartition, LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
//        case (topicPartition, _) => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
//      }
//
//      preparedOffsetsForPartitionsWithCommittedOffset ++ preparedOffsetsForPartitionsWithoutCommittedOffset
//    } else {
//      CommandLineUtils.printUsageAndDie(opts.parser, "Option '%s' requires one of the following scenarios: %s".format(opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts) )
//    }
//  }

}
