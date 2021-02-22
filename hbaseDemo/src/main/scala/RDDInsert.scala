package com.sparkbyexamples.spark.rdd.hbase.hortonworks

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

object RDDInsert {

  def main(args: Array[String]): Unit ={
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HBasePOC")
      .getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)
    try{
      val rowKey = "9"
      val rdd = spark.sparkContext.parallelize(Array(
        (Bytes.toBytes(rowKey),
        Array((Bytes.toBytes("person"), Bytes.toBytes ("firstName"), Bytes.toBytes ("Jack")))),
        (Bytes.toBytes(rowKey),
          Array((Bytes.toBytes("person"), Bytes.toBytes ("lastName"), Bytes.toBytes ("Jill")))),
      (Bytes.toBytes(rowKey),
        Array((Bytes.toBytes("person"), Bytes.toBytes ("middleName"), Bytes.toBytes ("M")))),
      (Bytes.toBytes(rowKey),
        Array((Bytes.toBytes("person"), Bytes.toBytes ("addressLine"), Bytes.toBytes ("1234 Lane")))),
      (Bytes.toBytes(rowKey),
        Array((Bytes.toBytes("person"), Bytes.toBytes ("city"), Bytes.toBytes ("Test")))),
      (Bytes.toBytes(rowKey),
        Array((Bytes.toBytes("person"), Bytes.toBytes ("state"), Bytes.toBytes ("FL")))),
      (Bytes.toBytes(rowKey),
        Array((Bytes.toBytes("person"), Bytes.toBytes ("zipCode"), Bytes.toBytes ("1234"))))
      ))

      rdd.hbaseBulkPut(hbaseContext, TableName.valueOf("employee"),
        (putRecord) => {
          val put = new Put(putRecord._1)
            putRecord._2.foreach((putvalue) => put.addColumn(putvalue._1, putvalue._2, putvalue._3))
            put

        })
      println("Record successfully inserted")
    }finally {
     spark.sparkContext.stop()
    }
  }

}
