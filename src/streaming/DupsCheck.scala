/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.streaming.examples

import java.sql.Date

import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.streaming.StreamSQLContext
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by Eravelly.Shiva.Kumar on 5/21/2016.
  */
object DupsCheck {
  case class SingleWord(word: String)
  case class User(id: String, dateofprogramentry: String, dateoffirstcasemanagement: String,
                  employmentservice: String, dateofprogramexit: String)
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[10]", "test", Duration(3000))
    val sc = ssc.sparkContext

    val streamSqlContext = new StreamSQLContext(ssc, new SQLContext(sc))
    import streamSqlContext._
    val lines = sc.textFile("C:\\DataServices\\stream1.csv")
    //Schema for DupCheck1
      val pirlDataDupCheck1 = lines.map(_.split(",")).map(w => User(w(0).toString.concat(w(1).toString), w(1).toString, w(2).toString, w(3).toString, w(4).toString))
    val DupCheck1Stream = new ConstantInputDStream[User](ssc, pirlDataDupCheck1)
    registerDStreamAsTable(DupCheck1Stream, "DupCheck1")
    //Schema for DupCheck 2 & 3
    val pirlDataDupCheck2n3 = lines.map(_.split(",")).map(w => User(w(0).toString, w(1).toString, w(2).toString, w(3).toString, w(4).toString))
    val DupCheck2n3Stream = new ConstantInputDStream[User](ssc, pirlDataDupCheck2n3)
    registerDStreamAsTable(DupCheck2n3Stream, "DupCheck2n3")
    //UDFS for DupCHeck 2
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
    //DupCheck1 Results
    sql("SELECT id, count(id) FROM DupCheck1 group by id having count(id) > 1").foreachRDD { r => r.foreach(println) }
    //DupCheck2 Results
    streamSqlContext.sql(
      """
        |SELECT distinct a.id, a.dateofprogramentry, a.dateoffirstcasemanagement, a.employmentservice, a.dateofprogramexit FROM DupCheck2n3 a, DupCheck2n3 b
        |WHERE a.id = b.id AND convetToDate(a.dateoffirstcasemanagement) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit) AND convetToDate(a.employmentservice) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit) AND convetToDate(a.dateofprogramexit) BETWEEN convetToDate(b.dateofprogramentry) AND addDays(b.dateofprogramexit)
      """.stripMargin).foreachRDD { r => r.foreach(println) }
    //DupCheck3 results
    streamSqlContext.sql("select id, max(dateofprogramentry) AS dpe, max(dateoffirstcasemanagement) AS dfcm, max(employmentservice) AS es from DupCheck2n3 where dateofprogramentry IS NOT NULL and dateoffirstcasemanagement IS NOT NULL group by id having count(id) > 1").foreachRDD { r => r.foreach(println) }
    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
  }
}