package erOutput.Individual.ValuationBenefitReg

import org.apache.spark.sql.types._

object ValuationBenefitRegSchema {
    val schemaIndv = StructType(List(
        StructField("datacenter", StringType, true),
        StructField("jobid", LongType, false),
        StructField("valuationbenefitkey", LongType, true),
        StructField("employeekey", IntegerType, true),
        StructField("combinedkey", IntegerType, true),
        StructField("executionid", LongType, true),
        StructField("majorbreakkeyid", IntegerType, true),
        StructField("benefittypekey", ShortType, true),
        StructField("decrementtypeid", ShortType, true),
        StructField("payoutbenefittypeid", ShortType, true),
        StructField("payoutbenefittypename", StringType, true),
        StructField("trancehetypename", StringType, true),
        StructField("prioritycategory", ShortType, true),
        StructField("liabilitytype", StringType, true),
        StructField("liabilitysubtype", StringType, true),
        StructField("tranchetypekey", IntegerType, true),
        StructField("decrementtype", StringType, true),
        StructField("benefittype", StringType, true),
        StructField("status", StringType, true),
        StructField("datacount", IntegerType, true),
        StructField("accruedliability", DecimalType(15,3), true),
        StructField("normalcost", DecimalType(15,3), true),
        StructField("presentvaluebenefits", DecimalType(15,3), true),
        StructField("expectedbenefitpayments", DecimalType(15,3), true),
        StructField("weightedliability", DecimalType(15,3), true),
        StructField("includeforliabilitytype", BooleanType, true),
        StructField("oneyearprojectedaccruedliability", DecimalType(15,3), true),
        StructField("termcost", DecimalType(15,3), true)
    ))
}
