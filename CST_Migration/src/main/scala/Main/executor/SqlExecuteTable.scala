package Main.executor

import Main.global
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object SqlExecuteTable {

  def executeTable(spark: SparkSession, jdbc_url: String, srcTable: String, connectionProperties: Properties): Unit = {
    val source: DataFrame = spark.read
      .jdbc(
        url = jdbc_url,
        table = f"(select * from ${global.srcSchema}.$srcTable) t",
        properties = connectionProperties
      )

    Utils.writeToBigQuery(source, "", "", "", srcTable)
  }
}
