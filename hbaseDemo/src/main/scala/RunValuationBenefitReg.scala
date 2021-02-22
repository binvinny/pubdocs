package erOutput.Aggregation.Regular.ValuationBenefitReg

import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.SparkSession

object RunValuationBenefitReg {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("HBasePOC")
      .getOrCreate()
    import spark.implicits._

    spark.conf.set("spark.debug.maxToStringFields",6000)
    // Read the file and convert to RDD

   /* val data = Seq(
                  ValuationBenefitReg("FKL",205818,2,1,25953421,23,-1)
                  )*/
  val data = Seq(
    ValuationBenefitReg("FKL",205818,1,1,25953421,23,-1,3,1,0,"test","default",99,"PPA_NAR_MIN","",0,"","PensionerPVnonLS","Retired",1,53015,0,0,0,0,"false",0,0)
  )

     val filename =  "Ind_205818.txt"
   // val rdd  = spark.sparkContext.textFile("/home/ramesh/projects/hbaseDemo/src/scala/Ind_205818.txt")
   //val data = Seq(NewEmployee("10", "E0004", "Smith", "K", "3456 main", "Orlando", "FL", "45235","45235","45235"),
    // NewEmployee("11", "E0006", "Williams", "L", "123 Grange Rd", "Newark", "NJ", "27656", "27656","45235"),
     //NewEmployee("12", "E0007", "Davis", "P", "Warners", "Sanjose", "CA", "34789", "34789","45235"))
 // val data = Seq(Employee("4", "E0004", "Smith", "K", "3456 main", "Orlando", "FL", "45235"),
   // Employee("6", "E0006", "Williams", "L", "123 Grange Rd", "Newark", "NJ", "27656"),
    //Employee("7", "E0007", "Davis", "P", "Warners", "Sanjose", "CA", "34789"))


    val df = spark.sparkContext.parallelize(data).toDF
    df.show(1)
    df.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalogValReg, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    println("Employee Data Saved Successfully");

    println("Load Data now ")

    val hbaseDF = spark.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalogValReg))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    //val indFileName = configManager.getString("Ind_205818.txt")
    hbaseDF.printSchema()
    //Collect and show Data from DataFrame
    hbaseDF.show(false)
    println("Completed Loading  Data now ")
    //val indRDD: RDD[String] = FlatFile.convertRDDWitKey(spark, ValuationBenefitRegSchema.schemaIndv, filename)
    //println("coming ")
   // ValuationBenefitRegTask.processIndvOutput(indRDD,spark)


  }

  def catalogValReg =
    s"""{
       |"table":{"name":"valuationbenefitreg"},
       |"rowkey":"valuationbenefitkey",
       |"columns":{
       |"ValuationBenefitKey":{"cf":"rowkey","col":"valuationbenefitkey","type":"long"},
       |"DataCenter":{"cf":"datacolumns","col":"datacenter","type":"string"},
       |"EmployeeKey":{"cf":"datacolumns","col":"employeekey","type":"integer"},
       |"CombinedKey":{"cf":"datacolumns","col":"combinedkey","type":"integer"},
       |"executionId":{"cf":"datacolumns","col":"executionid","type":"long"},
       |"MajorBreakKeyId":{"cf":"datacolumns","col":"majorbreakkeyid","type":"integer"},
       |"BenefitTypeKey":{"cf":"datacolumns","col":"benefittypekey","type":"integer"},
       |"DecrementTypeID":{"cf":"datacolumns","col":"decrementtypeid","type":"integer"},
       |"PayoutBenefitTypeID":{"cf":"datacolumns","col":"payoutbenefittypeid","type":"integer"},
       |"PayoutBenefitTypeName":{"cf":"datacolumns","col":"payoutbenefittypename","type":"string"},
       |"TrancheTypeName":{"cf":"datacolumns","col":"trancehetypename","type":"string"},
       |"PriorityCategory":{"cf":"datacolumns","col":"prioritycategory","type":"integer"},
       |"liabilityType":{"cf":"datacolumns","col":"liabilitytype","type":"string"},
       |"liabilitySubType":{"cf":"datacolumns","col":"liabilitysubtype","type":"string"},
       |"TrancheTypeKey":{"cf":"datacolumns","col":"tranchetypekey","type":"integer"},
       |"DecrementType":{"cf":"datacolumns","col":"decrementtype","type":"string"},
       |"BenefitType":{"cf":"datacolumns","col":"benefittype","type":"string"},
       |"Status":{"cf":"datacolumns","col":"Status","type":"string"},
       |"DataCount":{"cf":"datacolumns","col":"datacount","type":"integer"},
       |"AccruedLiability":{"cf":"datacolumns","col":"accruedliability","type":"integer"},
       |"NormalCost":{"cf":"datacolumns","col":"normalcost","type":"integer"},
       |"PresentValueBenefits":{"cf":"datacolumns","col":"presentvaluebenefits","type":"integer"},
       |"expectedBenefitPayments":{"cf":"datacolumns","col":"expectedbenefitpayments","type":"integer"},
       |"WeightedLiability":{"cf":"datacolumns","col":"Weightedliability","type":"integer"},
       |"IncludeForLiabilityType":{"cf":"datacolumns","col":"includeforliabilitytype","type":"string"},
       |"OneYearProjectedAccruedLiability":{"cf":"datacolumns","col":"oneYearprojectedaccruedliability","type":"integer"},
       |"TermCost":{"cf":"datacolumns","col":"termcost","type":"integer"},
       |"JobId":{"cf":"datacolumns","col":"jobid","type":"long"}
       |}
       |}""".stripMargin
}
