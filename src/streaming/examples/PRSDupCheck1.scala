package org.apache.spark.sql.streaming.examples

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Eravelly.Shiva.Kumar on 5/21/2016.
  */
object PRSDupCheck1 {
  case class SingleWord(word: String)
  case class User(id: String, dateofprogramentry: String, dateoffirstcasemanagement: String,
                  employmentservice: String, dateofprogramexit: String)
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[11]", "test", Duration(3000))
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._
    val lines = sc.textFile("C:\\DataServices\\stream1.csv").map(_.split(",")).map(w => User(w(0).toString.concat(w(1).toString), w(1).toString, w(2).toString, w(3).toString, w(4).toString))
    val dummyStream = new ConstantInputDStream[User](ssc, lines)
    registerDStreamAsTable(dummyStream, "test")

    sql("SELECT id, count(id) FROM test group by id having count(id) > 1").foreachRDD { r => r.foreach(println) }

    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }
}
