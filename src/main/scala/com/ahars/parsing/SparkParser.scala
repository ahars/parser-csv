package com.ahars.parsing

import java.text.SimpleDateFormat

/**
  * Created by ahars on 24/11/2015.
  */
class SparkParser extends Serializable {

  val Pattern1 = "(\\d{4}-\\d{2}-\\d{2})".r
  val format1 = new SimpleDateFormat("yyyy-MM-dd")

  def parseDate(a: String) = a match {
    case Pattern1(a) => Some(new java.sql.Date(format1.parse(a).getTime))
    case _ => None
  }

  def parseBoolean(a: String) = a match {
    case "true" => Some(true)
    case "false" => Some(false)
    case _ => None
  }

}
