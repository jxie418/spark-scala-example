package com.spark.example

import java.util.Optional
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SampleStreamingSource extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister {

  override def shortName(): String = "sample"

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    println(s"SampleStreamingSource.createMicroBatchReader() Called - \n\tSchema = $schema")
    new SampleMicroBatchReader(options)
  }
}

class SampleMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Logging {
  private val batchSize = options.get("batchSize").orElse("5").toInt
  private val producerRate = options.get("producerRate").orElse("5").toInt
  private val queueSize = options.get("queueSize").orElse("512").toInt

  private val NO_DATA_OFFSET = SampleOffset(-1)
  private var stopped: Boolean = false

  private var startOffset: SampleOffset = SampleOffset(-1)
  private var endOffset: SampleOffset = SampleOffset(-1)

  private var currentOffset: SampleOffset = SampleOffset(-1)
  private var lastReturnedOffset: SampleOffset = SampleOffset(-2)
  private var lastOffsetCommitted : SampleOffset = SampleOffset(-1)

  private val dataList: ListBuffer[String] = new ListBuffer[String]()
  private val dataQueue: BlockingQueue[String] = new ArrayBlockingQueue(queueSize)

  private var producer: Thread = _
  private var consumer: Thread = _

  initialize()

  def initialize(): Unit = {

    producer = new Thread("Sample Producer") {
      setDaemon(true)
      override def run() {
        val message: String =
          """
            |{"IsZip":true,"Content":"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","Hmac":"S+BQW9CNeNTp5rtbeNEe+g=="}
          """.stripMargin
        while(!stopped) {
          dataQueue.put(message)
          Thread.sleep(producerRate)
        }
      }
    }
    producer.start()

    consumer = new Thread("Sample Consumer") {
      setDaemon(true)
      override def run() {
        while (!stopped) {
          val message = dataQueue.poll(100, TimeUnit.MILLISECONDS)
          if (message != null.asInstanceOf[String]) {
            dataList.append(message)
            currentOffset = currentOffset + 1
          }
        }
      }
    }
    consumer.start()
  }

  override def deserializeOffset(json: String): Offset = SampleOffset(json.toLong)

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    this.startOffset = start.orElse(NO_DATA_OFFSET).asInstanceOf[SampleOffset]
    this.endOffset = end.orElse(currentOffset).asInstanceOf[SampleOffset]
  }

  override def getStartOffset: Offset = {
    if (startOffset.offset == -1) {
      throw new IllegalStateException("startOffset is -1")
    }
    startOffset
  }

  override def getEndOffset: Offset = {
    if (endOffset.offset == -1) {
      currentOffset
    } else {
      if (lastReturnedOffset.offset < endOffset.offset) {
        lastReturnedOffset = endOffset
      }
      endOffset
    }
  }

  override def commit(end: Offset): Unit = {
    val newOffset = SampleOffset.convert(end).getOrElse(
      sys.error(s"SampleStreamMicroBatchReader.commit() received an offset ($end) that did not " +
        s"originate with an instance of this class")
    )
    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt
    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }
    dataList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def createDataReaderFactories(): java.util.List[DataReaderFactory[Row]] = {
    synchronized {
      val startOrdinal = startOffset.offset.toInt + 1
      val endOrdinal = endOffset.offset.toInt + 1

      val newBlocks = synchronized {
        val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
        val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
        assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
        dataList.slice(sliceStart, sliceEnd)
      }

      newBlocks.grouped(batchSize).map { block =>
        new SampleStreamBatchTask(block).asInstanceOf[DataReaderFactory[Row]]
      }.toList.asJava
    }
  }


  override def readSchema(): StructType = StructType(StructField("Message", StringType, nullable = true):: Nil)

  override def stop(): Unit = stopped = true
}

class SampleStreamBatchTask(dataList: ListBuffer[String])
  extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = new SampletreamBatchReader(dataList)
}

class SampletreamBatchReader(dataList: ListBuffer[String]) extends DataReader[Row] {
  private var currentIdx = -1

  override def next(): Boolean = {
    currentIdx += 1
    currentIdx < dataList.size
  }

  override def get(): Row = Row(dataList(currentIdx))

  override def close(): Unit = ()
}