package Main

object DatePattern {
  private val Y   = """^\d{4}$""".r
  private val YM  = """^\d{6}$""".r
  private val YMD = """^\d{8}$""".r

  /** Return "yyyy", "yyyyMM", "yyyyMMdd", or "invalid". */
  def classify(s: String): (String, String) = s match {
    case YMD()   => ("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP($, 'yyyyMMdd')))", "DAY")
    case YM()  => ("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat($, '01'), 'yyyyMMdd')))", "MONTH")
    case Y() => ("TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(concat($, '0101'), 'yyyyMMdd')))", "YEAR")
    case _     => throw new Exception("un-supported type")
  }
}
