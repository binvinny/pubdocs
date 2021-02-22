package erOutput.utils


/**
  * This class is used to convert the value
  * since spark converts Dataframe doesn't support null
  */
object Converter {
    /**
      * convert null to 0 for decimal
      * @param value
      * @return
      */
    def parseNullableDecimal(value: java.math.BigDecimal): BigDecimal = {
        if (value == null)
            0.0
        else
            value
    }

    /**
      * convert null to 0 for int
      * @param value
      * @return
      */
    def parseNullableInt(value: java.lang.Integer): Int = {
        if (value == null)
            0
        else
            value
    }
  /**
    * convert null to 0 for short
    * @param value
    * @return
    */
  def parseNullableShort(value: java.lang.Short): Short = {
    if (value == null)
      0
    else
      value
  }
  /**
    * convert null to 0 for long
    * @param value
    * @return
    */
  def parseNullableLong(value: java.lang.Long): Long = {
    if (value == null)
      0
    else
      value
  }

    /**
      * convert null to false for Boolean
      * @param value
      * @return
      */
    def parseNullableBoolean(value: java.lang.Boolean) : Boolean = {
        if (value == null)
            false
        else
            value
    }
}
