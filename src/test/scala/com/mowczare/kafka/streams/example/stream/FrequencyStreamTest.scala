package com.mowczare.kafka.streams.example.stream

import com.madewithtea.mockedstreams.MockedStreams
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.pds.yahooWrappers.{HllWrap, ItemSketchWrap}
import com.yahoo.sketches.frequencies.ErrorType
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.{FunSuite, Matchers}

class FrequencyStreamTest extends FunSuite with Matchers {

  val admissibleError = 0.1

  val inputTestTopic = "input-test"
  val outputTestTopic = "output-test"

  def almostEqual(result: Double, shouldBe: Double): Boolean = {
    Math.abs(result - shouldBe) / shouldBe < admissibleError
  }

  test("Sample test") {
    val inputRecords: Seq[(String, InputEvent)] = Seq(
      ("test", InputEvent(4))
    ) ++ ((1 to 100).map(_ => 1) ++ (1 to 100)).map(v => v *2 + 1).map(v => "test" -> InputEvent(v))

    val streamResult = MockedStreams()
      .topology(ExampleStream.streamTopologyFrequency(4)(inputTestTopic, outputTestTopic))
      .input(inputTestTopic, Serdes.String, SerdeUtil.codecToSerde[InputEvent], inputRecords)
      .output[Long, ItemSketchWrap[InputEvent]](outputTestTopic, Serdes.Long, SerdeUtil.codecToSerde[ItemSketchWrap[InputEvent]], 1000)

    val finalResult = streamResult.groupBy(_._1).mapValues(_.last._2.itemSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES))

    finalResult(0).iterator.map(_.getItem).toList shouldBe List(InputEvent(4))
    finalResult(1).iterator.map(_.getItem).toList shouldBe List(InputEvent(3))
  }

}
