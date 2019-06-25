package com.mowczare.kafka.streams.hll.hashing

import java.nio.ByteBuffer
import java.nio.charset.Charset

import com.avsystem.commons.serialization.{ListOutput, ObjectOutput, Output, SimpleOutput}

import scala.collection.mutable

class Value2BytesOutput(accumulator: mutable.AbstractBuffer[Byte])
  extends Output {
  override def writeNull(): Unit = {}
  override def writeSimple(): SimpleOutput = new SimpleOutput {

    override def writeString(str: String): Unit = {
      accumulator ++= str.getBytes(Charset.forName("UTF-8"))
    }

    override def writeBoolean(boolean: Boolean): Unit =
      accumulator += (if (boolean) 1 else 0)

    override def writeInt(int: Int): Unit =
      accumulator ++= ByteBuffer.allocate(4).putInt(int).array()

    override def writeLong(long: Long): Unit =
      accumulator ++= ByteBuffer.allocate(8).putLong(long).array()

    override def writeDouble(double: Double): Unit =
      ByteBuffer.allocate(8).putDouble(double).array()

    override def writeBigInt(bigInt: BigInt): Unit =
      accumulator ++= bigInt.toByteArray

    override def writeBigDecimal(bigDecimal: BigDecimal): Unit =
      accumulator ++= ByteBuffer.allocate(4).putInt(bigDecimal.hashCode).array()

    override def writeBinary(binary: Array[Byte]): Unit = accumulator ++= binary

  }
  override def writeList(): ListOutput = new Value2BytesListOutput(this)
  override def writeObject(): ObjectOutput = new Value2BytesObjectOutput(this)
}

class Value2BytesListOutput(value2BytesOutput: Value2BytesOutput)
  extends ListOutput {

  override def writeElement(): Output = value2BytesOutput

  override def finish(): Unit = {}
}

class Value2BytesObjectOutput(value2BytesOutput: Value2BytesOutput)
  extends ObjectOutput {

  override def writeField(key: String): Output = value2BytesOutput

  override def finish(): Unit = {}
}

