package org.apache.spark.sql.streaming.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.sql.streaming.examples.DupsCheck.User
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Eravelly.Shiva.Kumar on 5/21/2016.
  */
object PRSDupChecks3 {
  case class User(id: String, dateofprogramentry: String, dateoffirstcasemanagement: String,
                  employmentservice: String, dateofprogramexit: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DupsCheck")
    //val ssc = new StreamingContext(sparkConf, Seconds(5))
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext
    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    val lines = sc.textFile("C:\\DataServices\\stream.csv").map(_.split(",")).map(w => User(w(0).toString, w(1).toString, w(2).toString, w(3).toString, w(4).toString))
    val userStream1 = streamSqlContext.createSchemaDStream(
      new ConstantInputDStream[User](ssc, lines))
    println("userStream Data------------------------------------------>>>>>>>>")
    //userStream1.foreachRDD { r => r.foreach(println) }
    streamSqlContext.registerDStreamAsTable(userStream1, "user")

    //Dup Check 2
    println("output output output------------------------------------------>>>>>>>>")
    streamSqlContext.sql("select id, max(dateofprogramentry) AS dpe, max(dateoffirstcasemanagement) AS dfcm, max(employmentservice) AS es from user where dateofprogramentry IS NOT NULL and dateoffirstcasemanagement IS NOT NULL group by id having count(id) > 1")
        .registerAsTable("user1")
    //streamSqlContext.sql("select id, dpe, dfcm from user1")
     // .foreachRDD { r => r.foreach(println) }
    streamSqlContext.sql("select a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit from user a, user1 b where a.id=b.id and a.dateofprogramentry <> b.dpe and a.dateoffirstcasemanagement <> b.dfcm and a.employmentservice <> b.es and a.dateofprogramexit IS NULL OR a.dateofprogramexit = ''").foreachRDD { r => r.foreach(println) }
    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }

}
