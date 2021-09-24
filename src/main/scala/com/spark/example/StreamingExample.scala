package com.spark.example

import java.time.OffsetDateTime
import java.util.concurrent.TimeUnit

import com.dexcom.models._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import RealTimeEntryRecords.GlucoseType
import RealTimeEntryRecords.MeterRecordType

import scala.util.{Failure, Success, Try}

object StreamingExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    val stringDecoder = Encoders.STRING
    implicit val realTimeRecordsTryEncoders = Encoders.kryo[Try[RealTimeRecord]]

    implicit val encoderv2 = Encoders.product[RealTimeRecord]
    implicit val encoderv3 = Encoders.product[RealTimeEvent]
   implicit  val encoders4 = Encoders.product[RealTimeGlucoseEventRecord]
    implicit val encoders5 = Encoders.product[RealTimeMeterRecord]

    val schema = StructType(StructField("message", StringType, nullable = true) :: Nil)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .getOrCreate()

    val sampleStream = spark.readStream
      .format("sample")
      .option("producerRate", "5")
      .option("queueSize", "100")
      .option("batchSize", "25")
      .schema(schema)
      .load()

    sampleStream.printSchema()

    val totalEvents = sampleStream
      .flatMap{
        r=>
          val parsed = RealTimeEntryRecords.asRealTimeRecord(r.getAs[String]("Message"))
          parsed match {
            case Success(r) => Some(r)
            case Failure(_) => None
          }
      }.flatMap(r=>r.Events)


     val glucoseData= totalEvents.filter(r=>r.EventType.equals(GlucoseType)).flatMap{
      r=> r.asEventData.map(r=>r.asInstanceOf[RealTimeGlucoseEventRecord])
    }



    glucoseData.printSchema()

    val meterData = totalEvents.filter(r=>r.EventType.equals(MeterRecordType)).flatMap{
      r=> r.asEventData.map(r=>r.asInstanceOf[RealTimeMeterRecord])
    }

    val query: StreamingQuery = glucoseData
      .writeStream
      .outputMode("append")
      .queryName("glucose")
      .format("memory")
      .start()

    for(_ <- 0 to 10) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(2))
      spark.sql("select * from glucose").show(false)
    }

    val query2: StreamingQuery = meterData
      .writeStream
      .outputMode("append")
      .queryName("meter")
      .format("memory")
      .start()

    for(_ <- 0 to 10) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(2))
      spark.sql("select * from meter").show(false)
    }

    query.stop()
    query2.stop()
  }
}