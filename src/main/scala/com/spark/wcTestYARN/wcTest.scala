package com.spark.wcTestYARN

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object wcTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
//    val process = Runtime.getRuntime.exec("cmd /c rd E:\\ABCD\\out001 /s/q")
    val session = SparkSession.builder().appName("wcTest")
//        .master("local")
      .getOrCreate()
    session.sparkContext.textFile(args(0))
      .flatMap(_.split("[:,\"\"\"]"))
      .map((_,1))
      .reduceByKey(_+_)
        .repartition(1)
      .sortBy(_._2,false)
      .saveAsTextFile(args(1))
    session.stop()
  }
}
