package erOutput.Aggregation.Regular.ValuationBenefitReg


import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.DataFrame

//import erOutput.utils.Base.BaseSingleton
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ValuationBenefitRegTask {

  // method to process each Individual ER Output message
  // each ER Output message could contain multiple records from the same job and participant
  // This method will:
  //   1. Cache the messages to a List of String (or anything better)
  //   2. when the size of the List reaches the limit (batch size), parse the message and save them into Hive
  //       individual table in batch
  def processIndvOutput(rdd: RDD[String], spark: SparkSession): Unit = {
    if (rdd.isEmpty())
      return
    try {

/**
      // Map rdd to dataFrame
      val dfRaw = rdd.map(x => {
        parseObject(x)
      }).map(valueJson => {
        val dataCenter = valueJson.getString("datacenter")
        val jobId = valueJson.getLong("jobid")
        val valuationBenefitKey = valueJson.getLong("valuationbenefitkey")
        val employeeKey = valueJson.getInteger("employeekey")
        val combinedKey = valueJson.getInteger("combinedkey")
        val executionId = valueJson.getLong("executionid")
        val majorBreakKeyId = valueJson.getInteger("majorbreakkeyid")
        val benefitTypeKey = valueJson.getShort("benefittypekey")
        val decrementTypeID = valueJson.getShort("decrementtypeid")
        val payoutBenefitTypeID = valueJson.getShort("payoutbenefittypeid")
        val payoutBenefitTypeName = valueJson.getString("payoutbenefittypename")
        val trancheTypeName = valueJson.getString("tranchetypename")
        val priorityCategory = valueJson.getShort("prioritycategory")
        val liabilityType = valueJson.getString("liabilitytype")
        val trancheTypeKey = valueJson.getInteger("tranchetypekey")
        val decrementType = valueJson.getString("decrementtype")
        val benefitType = valueJson.getString("benefittype")
        val status = valueJson.getString("status")
        val dataCount = valueJson.getInteger("datacount")
        val accruedLiability = valueJson.getBigDecimal("accruedliability")
        val normalCost = valueJson.getBigDecimal("normalcost")
        val presentValueBenefits = valueJson.getBigDecimal("presentvaluebenefits")
        val expectedBenefitPayments = valueJson.getBigDecimal("expectedbenefitpayments")
        val weightedLiability = valueJson.getBigDecimal("weightedliability")
        val includeForLiabilityType = valueJson.getBoolean("includeforliabilitytype")
        val oneYearProjectedAccruedLiability = valueJson.getBigDecimal("oneyearprojectedaccruedliability")
        val termCost = valueJson.getBigDecimal("termcost")

        ValuationBenefitReg(
          dataCenter,
          jobId,
          valuationBenefitKey,
          Converter.parseNullableInt(employeeKey),
          Converter.parseNullableInt(combinedKey),
          executionId,
          Converter.parseNullableInt(majorBreakKeyId),
          benefitTypeKey,
          decrementTypeID,
          payoutBenefitTypeID,
          payoutBenefitTypeName,
          trancheTypeName,
          priorityCategory,
          liabilityType,
          liabilityType, // liabilitySubType is same with liabilityType
          Converter.parseNullableInt(trancheTypeKey),
          decrementType,
          benefitType,
          status,
          Converter.parseNullableInt(dataCount),
          Converter.parseNullableDecimal(accruedLiability),
          Converter.parseNullableDecimal(normalCost),
          Converter.parseNullableDecimal(presentValueBenefits),
          Converter.parseNullableDecimal(expectedBenefitPayments),
          Converter.parseNullableDecimal(weightedLiability),
          Converter.parseNullableBoolean(includeForLiabilityType),
          Converter.parseNullableDecimal(oneYearProjectedAccruedLiability),
          Converter.parseNullableDecimal(termCost)
        )
      }).toDF("DataCenter", "JobId", "ValuationBenefitKey", "EmployeeKey", "CombinedKey", "executionId", "MajorBreakKeyId", "BenefitTypeKey", "DecrementTypeID", "PayoutBenefitTypeID", "PayoutBenefitTypeName", "TrancheTypeName", "PriorityCategory", "liabilityType", "liabilitySubType", "TrancheTypeKey", "DecrementType", "BenefitType", "Status", "DataCount", "AccruedLiability", "NormalCost", "PresentValueBenefits", "expectedBenefitPayments", "WeightedLiability", "IncludeForLiabilityType", "OneYearProjectedAccruedLiability", "TermCost")

        saveResult(spark,dfRaw)
**/
      //ValuationBenefitRegSaveToHive.flushIndvResult(spark, dfRaw)
      //logManager.logInfo(spark, 0, "end indiv ValuationBenefitReg")
    }
    catch {
      case e: Exception => {
        //logManager.logInfo(spark, 0, "ValuationBenefitReg Indiv Exception: " + e.getMessage)
      }
    }


  }
  def catalog =
    s"""{
       |"table":{"name":"valuationbenefitreg"},
       |"ValuationBenefitKey":"key",
       |"columns":{
       |"ValuationBenefitKey":{"cf":"rowkey","col":"key","type":"string"},
       |"DataCenter":{"cf":"datacolumns","col":"datacenter","type":"string"},
       |"EmployeeKey":{"cf":"datacolumns","col":"employeekey","type":"string"},
       |"CombinedKey":{"cf":"datacolumns","col":"combinedkey","type":"string"},
       |"executionId":{"cf":"datacolumns","col":"executionid","type":"string"},
       |"MajorBreakKeyId":{"cf":"datacolumns","col":"majorbreakkeyid","type":"string"},
       |"BenefitTypeKey":{"cf":"datacolumns","col":"benefittypekey","type":"string"},
       |"DecrementTypeID":{"cf":"datacolumns","col":"decrementtypeid","type":"string"},
       |"PayoutBenefitTypeID":{"cf":"datacolumns","col":"payoutbenefittypeid","type":"string"}
       |"PayoutBenefitTypeName":{"cf":"datacolumns","col":"payoutbenefittypename","type":"string"},
       |"TrancheTypeName":{"cf":"datacolumns","col":"trancehetypename","type":"string"},
       |"PriorityCategory":{"cf":"datacolumns","col":"prioritycategory","type":"string"},
       |"liabilityType":{"cf":"datacolumns","col":"liabilitytype","type":"string"},
       |"liabilitySubType":{"cf":"datacolumns","col":"liabilitysubtype","type":"string"}
       |"TrancheTypeKey":{"cf":"datacolumns","col":"tranchetypekey","type":"string"},
       |"DecrementType":{"cf":"datacolumns","col":"decrementtype","type":"string"},
       |"BenefitType":{"cf":"datacolumns","col":"benefittype","type":"string"},
       |"Status":{"cf":"datacolumns","col":"Status","type":"string"},
       |"DataCount":{"cf":"datacolumns","col":"datacount","type":"string"}
       |"AccruedLiability":{"cf":"datacolumns","col":"accruedliability","type":"string"},
       |"NormalCost":{"cf":"datacolumns","col":"normalcost","type":"string"},
       |"PresentValueBenefits":{"cf":"datacolumns","col":"presentvaluebenefits","type":"string"},
       |"expectedBenefitPayments":{"cf":"datacolumns","col":"expectedbenefitpayments","type":"string"},
       |"WeightedLiability":{"cf":"datacolumns","col":"Weightedliability","type":"string"}
       |"IncludeForLiabilityType":{"cf":"datacolumns","col":"includeforliabilitytype","type":"string"},
       |"OneYearProjectedAccruedLiability":{"cf":"datacolumns","col":"oneYearprojectedaccruedliability","type":"string"},
       |"TermCost":{"cf":"datacolumns","col":"termcost","type":"string"},
       |"JobID":{"cf":"job","col":"jobid","type":"string"},
       |}""".stripMargin
  private def saveResult (spark: SparkSession, df:DataFrame)
  {
    val dfResults = df.select(
      "DataCenter",
      "ValuationBenefitKey",
      "EmployeeKey",
      "CombinedKey",
      "executionId",
      "MajorBreakKeyId",
      "BenefitTypeKey",
      "DecrementTypeID",
      "PayoutBenefitTypeID",
      "PayoutBenefitTypeName",
      "TrancheTypeName",
      "PriorityCategory",
      "liabilityType",
      "liabilitySubType",
      "TrancheTypeKey",
      "DecrementType",
      "BenefitType",
      "Status",
      "DataCount",
      "AccruedLiability",
      "NormalCost",
      "PresentValueBenefits",
      "expectedBenefitPayments",
      "WeightedLiability",
      "IncludeForLiabilityType",
      "OneYearProjectedAccruedLiability",
      "TermCost",
      "JobID"
    )
    //val df = spark.sparkContext.parallelize(dfResults).toDF
    dfResults.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}