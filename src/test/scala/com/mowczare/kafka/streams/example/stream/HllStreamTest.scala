package com.mowczare.kafka.streams.example.stream

import com.madewithtea.mockedstreams.MockedStreams
import com.mowczare.kafka.streams.example.model.InputEvent
import com.mowczare.kafka.streams.example.serde.SerdeUtil
import com.mowczare.kafka.streams.pds.hll.HllWrap
import org.apache.kafka.streams.scala.Serdes
import org.scalatest.{FunSuite, Matchers}

class HllStreamTest extends FunSuite with Matchers {
  import com.mowczare.kafka.streams.pds.hashing.GenCodecHashing._

  val admissibleError = 0.1

  val inputTestTopic = "input-test"
  val outputTestTopic = "output-test"

  def almostEqual(result: Double, shouldBe: Double): Boolean = {
    Math.abs(result - shouldBe) / shouldBe < admissibleError
  }

  test("Sample test") {
    val inputRecords: Seq[(String, InputEvent)] = Seq(
      ("test", InputEvent(1)),
      ("test", InputEvent(3)),
      ("test", InputEvent(5)),
      ("test", InputEvent(4))
    )

    val streamResult = MockedStreams()
      .topology(ExampleStream.streamTopologyHll(inputTestTopic, outputTestTopic))
      .input(inputTestTopic, Serdes.String, SerdeUtil.codecToSerde[InputEvent], inputRecords)
      .output[Long, HllWrap[InputEvent]](outputTestTopic, Serdes.Long, SerdeUtil.codecToSerde[HllWrap[InputEvent]], 1000)

    val finalResult = streamResult.groupBy(_._1).mapValues(_.last._2.hll.getEstimate)


    assert(almostEqual(finalResult(0),1))
    assert(almostEqual(finalResult(1),3))



  }

}
