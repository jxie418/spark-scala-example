package com.spark.example


case class OffsetInfo(topic: String, partition: Int, offset: Long)
object OffsetInfo {
  def apply(offsetString: String): OffsetInfo = {
    val offsetStrArray = offsetString.split(":")
    OffsetInfo(offsetStrArray(0), offsetStrArray(1).toInt, offsetStrArray(2).toLong)
  }

}
object ParsingKafkaOffsetString extends App {
  val kafkaOffsetString = "janus.raw_bulk_data_ready_topic:9:3399,janus.raw_bulk_data_ready_topic:16:1724,janus.raw_bulk_data_ready_topic:14:229,janus.raw_bulk_data_ready_topic:8:1989,janus.raw_bulk_data_ready_topic:19:2321,janus.raw_bulk_data_ready_topic:4:3022,janus.raw_bulk_data_ready_topic:3:1008,janus.raw_bulk_data_ready_topic:15:944252,janus.raw_bulk_data_ready_topic:0:12396,janus.raw_bulk_data_ready_topic:20:1808,janus.raw_bulk_data_ready_topic:18:298,janus.raw_bulk_data_ready_topic:2:693,janus.raw_bulk_data_ready_topic:23:1065,janus.raw_bulk_data_ready_topic:11:1044,janus.raw_bulk_data_ready_topic:5:1059,janus.raw_bulk_data_ready_topic:12:1441,janus.raw_bulk_data_ready_topic:21:1227,janus.raw_bulk_data_ready_topic:7:2137,janus.raw_bulk_data_ready_topic:1:360,janus.raw_bulk_data_ready_topic:10:2193,janus.raw_bulk_data_ready_topic:17:550688,janus.raw_bulk_data_ready_topic:6:14720,janus.raw_bulk_data_ready_topic:22:919,janus.raw_bulk_data_ready_topic:13:5196"
  val kafkaOffset = kafkaOffsetString.split(",").map(OffsetInfo(_)).map(_.offset).reduce((a, b)=>(a+b))
  println(kafkaOffset)
}
