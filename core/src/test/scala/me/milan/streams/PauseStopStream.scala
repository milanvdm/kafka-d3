package me.milan.streams

import scala.concurrent.duration._

import cats.effect.IO
import fs2.Stream
import org.scalatest.{Matchers, WordSpec}

class PauseStopStream  extends WordSpec with Matchers {

  "Streams" can {

    implicit val executor = scala.concurrent.ExecutionContext.global
    implicit val cs = IO.contextShift(executor)
    implicit val timer = IO.timer(executor)

    "be paused" should {

      "keep merged stream running" in {

        val pauseSignal = Stream.sleep(1.second).drain  ++ Stream.emit(true)
        val stopSignal = Stream.sleep(5.seconds).drain  ++ Stream.emit(true)

        val pauseStream = Stream.eval_(IO { println(1); Thread.sleep(500) }).repeat.pauseWhen(pauseSignal).interruptWhen(stopSignal).drain
        val infiniteStream = Stream.eval_(IO { println(2); Thread.sleep(500) }).repeat.interruptWhen(stopSignal)

        val result = pauseStream.merge(infiniteStream)

        result.compile.toList.unsafeRunSync()

      }
    }
  }
}
