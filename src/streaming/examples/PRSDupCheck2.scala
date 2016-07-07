package org.apache.spark.sql.streaming.examples
import java.sql.Date
//import java.util.Date
import
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Eravelly.Shiva.Kumar on 5/21/2016.
  */
object PRSDupCheck2 {
  case class User(id: String, dateofprogramentry: String, dateoffirstcasemanagement: String,
                  employmentservice: String, dateofprogramexit: String)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DupsCheck")
    //val ssc = new StreamingContext(sparkConf, Seconds(5))
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext
    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    val lines = sc.textFile("C:\\DataServices\\stream2.csv").map(_.split(",")).map(w => User(w(0).toString, w(1).toString, w(2).toString, w(3).toString, w(4).toString))
    val userStream1 = streamSqlContext.createSchemaDStream(
      new ConstantInputDStream[User](ssc, lines))

    //userStream1.foreachRDD { r => r.foreach(println) }
    streamSqlContext.registerDStreamAsTable(userStream1, "user")
    //Dup Check 2

    streamSqlContext.udf.register("addDays", (word: String) => {
      val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
      val date = format.parse(word)
      //println("Program Exit Date------------------------------------------>>>>>>>>"+new Date(date.getTime() + 90*24*60*60*1000))
      new Date(date.getTime() + 90*24*60*60*1000)
    })
    streamSqlContext.udf.register("convetToDate", (word: String) => {
      val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
      val date = format.parse(word)
      //println("Dates------------------------------------------>>>>>>>>"+new Date(date.getTime()))
      new Date(date.getTime())
    })
    streamSqlContext.sql(
      """
        |SELECT distinct a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit FROM user a, user b
        |WHERE a.id = b.id AND convetToDate(a.dateoffirstcasemanagement) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit) AND convetToDate(a.employmentservice) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit) AND convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit)
      """.stripMargin).foreachRDD { r => r.foreach(println) }
    /*streamSqlContext.sql(
      """
        |SELECT distinct a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit FROM user a, user b
        |WHERE a.id = b.id AND a.dateoffirstcasemanagement BETWEEN b.dateofprogramentry AND b.dateofprogramexit
      """.stripMargin).foreachRDD { r => r.foreach(println) }*/
    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }


}
