package com.spark.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.ClassTag
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkStructureStreaming {
  def main(args: Array[String]) {

   /* import org.apache.spark.sql.Encoders

    case class Foo(field: String)
    case class Wrapper(lb: scala.collection.mutable.ListBuffer[Foo])
    Encoders.product[Wrapper]

    Encoders.kryo[scala.collection.mutable.ListBuffer[Foo]]

    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    ExpressionEncoder[scala.collection.mutable.ListBuffer[Foo]]
    ExpressionEncoder[scala.collection.mutable.ListBuffer[Foo]].schema == ExpressionEncoder[List[Foo]].schema*/

    val stringencoder = Encoders.STRING
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder.config(conf).appName("hello spark").getOrCreate()

    val lines: DataFrame =  spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

    val words: Dataset[String] = lines.as[String](stringencoder).flatMap(_.split(" "))(stringencoder)
    val wordCounts: DataFrame = words.groupBy("value").count()
    val query: StreamingQuery =wordCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }
}
