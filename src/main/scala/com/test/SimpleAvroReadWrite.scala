package com.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._


/**
  * Created by Honda on 2017/11/7.
  */
object SimpleAvroReadWrite {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark Avro")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = Seq(
      (2012,8,"Batman",9.8),
      (2012,8,"Hero" , 8.7),
      (2011,7,"Git",2.0),
      (2011,6,"Wonder Women" ,10),
      (2015,4,"Iron man",9)
    ).toDF("year","month","title","rating")

    df.coalesce(1).write.avro("avrotest")

    // partiton by specific column
    df.write.partitionBy("year","month").avro("avrotest123")

    val dfReader = sqlContext.read.avro("avrotest/*.avro")
    dfReader.filter("rating > 5").foreach(println)


  }
}
