package com.sparkbyexamples.spark.rdd.hbase.hortonworks

import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.sql.SparkSession

object RDDGet {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HBasePOC")
      .getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)
    try {

      val rdd = spark.sparkContext.parallelize(Array(
        Bytes.toBytes("2")))

      val getRdd= rdd.hbaseBulkGet[String](hbaseContext,TableName.valueOf("valuationbenefitreg"), 2,
        record => {
          println("making a Get " + record)
          new Get(record)
        },(result: Result) => {
          val it = result.listCells().iterator()
          val b = new StringBuilder

          b.append(Bytes.toString(result.getRow) + ":")
          while (it.hasNext)
            {
              val cell = it.next()
              val q = Bytes.toString(CellUtil.cloneQualifier((cell)))

                b.append("(" + q + "," + Bytes.toString(CellUtil.cloneValue(cell)) + ")")

            }
           b.toString()
        }
      )
      getRdd.collect().foreach(v => println(v))
    }
    finally
      {
        spark.sparkContext.stop()
      }
  }
}
