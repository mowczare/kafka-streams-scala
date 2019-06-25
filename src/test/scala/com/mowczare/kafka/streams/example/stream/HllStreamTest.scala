package com.mowczare.kafka.streams.example.stream

import com.madewithtea.mockedstreams.MockedStreams
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.hll.model.HllWrap
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.{FunSuite, Matchers}
import com.mowczare.kafka.streams.hll.hashing.GenCodecHashing._

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
      .output[Long, HllWrap[InputEvent]](outputTestTopic, Serdes.Long, HllWrap.hllSerde[InputEvent], 1000) shouldBe IndexedSeq.empty
  }

}
