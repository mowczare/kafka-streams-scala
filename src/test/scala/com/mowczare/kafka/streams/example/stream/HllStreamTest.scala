package com.mowczare.kafka.streams.example.stream

import com.madewithtea.mockedstreams.MockedStreams
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.pds.yahooWrappers.ItemSketchWrap
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.{FunSuite, Matchers}

class HllStreamTest extends FunSuite with Matchers {

  val inputTestTopic = "input-test"
  val outputTestTopic = "output-test"

  test("Sample test") {
    val inputRecords: Seq[(String, InputEvent)] = Seq(
      ("test", InputEvent(1)),
      ("test", InputEvent(3)),
      ("test", InputEvent(5)),
      ("test", InputEvent(4))
    )

    MockedStreams()
      .topology(ExampleStream.streamTopologyHll(inputTestTopic, outputTestTopic))
      .input(inputTestTopic, Serdes.String, SerdeUtil.codecToSerde[InputEvent], inputRecords)
      .output[Long, ItemSketchWrap[InputEvent]](outputTestTopic, Serdes.Long, SerdeUtil.codecToSerde[ItemSketchWrap[InputEvent]], 1000) shouldBe IndexedSeq.empty
  }

}
