package erOutput.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Iterable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object FlatFile {
    /**
      * read from a flatFile with Json format then convert to RDD
      *
      * @param spark
      * @param schemaType
      * @param fileName
      * @return RDD[String] type
      */
    def convertRDD(spark: SparkSession, schemaType: StructType, fileName: String,parallelism: Option[Int]=None) = {
        val aggFileName = fileName
        var tblBufferList: ListBuffer[String] = ListBuffer[String]()
        for (line <- Source.fromFile(aggFileName).getLines) {
            tblBufferList += line
        }

        if(parallelism==None)
            spark.sparkContext.parallelize(tblBufferList)
        else
            spark.sparkContext.parallelize(tblBufferList,parallelism.get)

    }

    /**
      * read from a flatFile with Json format and contains the key with space  then convert to RDD
      *
      * @param spark
      * @param schemaType
      * @param fileName
      * @return RDD[String] type
      */
    def convertRDDWitKey(spark: SparkSession, schemaType: StructType, fileName: String) = {
        val aggFileName = fileName
        var tblBufferList: ListBuffer[String] = ListBuffer[String]()
        for (line <- Source.fromFile(aggFileName).getLines) {
            val words = line.split(" ")
            tblBufferList += words(1)
        }
        spark.sparkContext.parallelize(tblBufferList)
    }
    def convertRDDWitDashKey(spark: SparkSession, schemaType: StructType, fileName: String):Iterable[String] = {
        val aggFileName = fileName
        var tblBufferList: ListBuffer[String] = ListBuffer[String]()
        for (line <- Source.fromFile(aggFileName).getLines) {
            val words = line.split("-")
            tblBufferList += words(1)
        }
        tblBufferList
    }
    /**
      * read from a flatFile with Json format then convert to DataFrame
      *
      * @param spark
      * @param schemaType
      * @param fileName
      * @return DataFrame type
      */
    def convertDF(spark: SparkSession, schemaType: StructType, fileName: String,parallelism:Option[Int]=None): DataFrame = {
        val resultRDD = convertRDD(spark, schemaType, fileName,parallelism)
        spark.read.schema(schemaType).json(resultRDD)
    }

    /**
      * read from a flatFile with Json format and contains the key with space then convert to DataFrame
      *
      * @param spark
      * @param schemaType
      * @param fileName
      * @return DataFrame type
      */
    def convertDFWithKey(spark: SparkSession, schemaType: StructType, fileName: String): DataFrame = {
        val resultRDD = convertRDDWitKey(spark, schemaType, fileName)
        spark.read.schema(schemaType).json(resultRDD)
    }

    /**
      * Read file row by row and append the contents of the file in an array
      * @param fileName
      * @return
      */
    def readFile(fileName: String): Array[String] = {
        var result: Array[String] = Array[String]()
        for (line <- Source.fromFile(fileName).getLines) {
            result = result :+ line
        }
        result
    }

    /**
      * Read file that contains the key with space and append the contents of the file in an array
      * @param fileName
      * @return
      */
    def readFileWithKey(fileName: String): Array[String] = {
        var result: Array[String] = Array[String]()
        for (line <- Source.fromFile(fileName).getLines) {
            val words = line.split(" ")
            result = result :+ words(1)
        }
        result
    }
}