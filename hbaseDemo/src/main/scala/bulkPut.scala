package com.sparkbyexamples.spark.rdd.hbase.hortonworks

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
object bulkPut {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkHbasePOC")
      .getOrCreate()
    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)
    val rdd = spark.sparkContext.makeRDD(Array("1,218245,UseStabilizedRates,0.06,ActuarialReport,FKL,2182451",
      "1,218245,UseStabilizedRates,false,ActuarialReport,FKL,2182451",
      "3,218245,InterestRate,0.06,ActuarialReport,FKL,2182451",
      "4,218245,IncludePVYearsOrPay,false,ActuarialReport,FKL,2182451",
      "5,218245,UseStabilizedRates,false,ActuarialReport,FKL,2182451",
      "6,218245,InterestRate,0.06,ActuarialReport,FKL,2182451",
      "7,218245,IncludePVYearsOrPay,false,ActuarialReport,FKL,2182451", "218245,UseStabilizedRates,false,ActuarialReport,FKL,2182451",
      "8,218245,InterestRate,0.06,ActuarialReport,FKL,2182451",
      "9,218245,IncludePVYearsOrPay,false,ActuarialReport,FKL,2182451",
      "10,218245,UseStabilizedRates,false,ActuarialReport,FKL,2182451",
      "11,218245,InterestRate,0.06,ActuarialReport,FKL,2182451",
      "12,218245,IncludePVYearsOrPay,false,ActuarialReport,FKL,2182451"
    ))
  hbaseContext.bulkPut(rdd, TableName.valueOf("jobdetaileav"),bulkPutFunc)
    println("successfully bulk copy the records to jobdetail eav")
spark.sparkContext.stop()
  }
  def bulkPutFunc(arrayRec:String) :Put ={
    val rec = arrayRec.split(",")
    val put = new Put(Bytes.toBytes(rec(0).toInt))
    put.addColumn(Bytes.toBytes("attributes"),Bytes.toBytes("jobid"),Bytes.toBytes(rec(1)))
    put.addColumn(Bytes.toBytes("attributes"),Bytes.toBytes("attribute"),Bytes.toBytes(rec(2)))
    put.addColumn(Bytes.toBytes("attributes"),Bytes.toBytes("value"),Bytes.toBytes(rec(3)))
    put.addColumn(Bytes.toBytes("attributes"),Bytes.toBytes("category"),Bytes.toBytes(rec(4)))
    put.addColumn(Bytes.toBytes("attributes"),Bytes.toBytes("datacenter"),Bytes.toBytes(rec(5)))
    //put.addColumn(Bytes.toBytes("globaljobid"),Bytes.toBytes("globaljobid"),Bytes.toBytes(rec(6)))
    put

  }

}
