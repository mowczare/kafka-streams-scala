package com.mowczare.kafka.streams.example.stream

import com.madewithtea.mockedstreams.MockedStreams
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.{FunSuite, Matchers}

class ExampleStreamTest extends FunSuite with Matchers {

  val inputTestTopic = "input-test"
  val outputTestTopic = "output-test"

  test("Sample test") {
    val inputRecords: Seq[(String, InputEvent)] = Seq(
      ("test", InputEvent(42))
    )

    MockedStreams()
      .topology(ExampleStream.streamTopology(inputTestTopic, outputTestTopic))
      .input(inputTestTopic, Serdes.String, SerdeUtil.codecToSerde[InputEvent], inputRecords)
      .output(outputTestTopic, Serdes.String, Serdes.String, 1000) shouldBe IndexedSeq.empty
  }
}
