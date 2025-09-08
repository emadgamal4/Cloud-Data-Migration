package Main.executor

import Main.global
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Connection

object ACIDExecuteTable {

  def executeTable(spark: SparkSession, conn: Connection, srcTable: String, config: DataFrame): Unit = {
    val last_offset = Utils.checkTableInConfig(config, srcTable)
    val postfixQuery = if (last_offset == null) "" else s" where CTL_LAST_SOURCE_UPDATE <= $last_offset"

    val trgTable = srcTable + "_bigQuery_temp"

    // create temp table from ORC ACID Transaction Table
    val isCreated = Utils.createTmp(conn, srcTable, trgTable)

    // extract partition column and clustering
    val combined = Utils.getPartitionAndClustering(spark, srcTable)
    val partition = combined._1  // must be one
    val bucketCols = combined._2

    // read source to get partitionType and ToDate Function
    if (partition != null && partition.nonEmpty) {
      var source = spark.sql(f"select * from ${global.srcSchema}.$trgTable where $partition is not null limit 1")

      if (source.collect().length > 0){
        val partitionCombined = Utils.getPartitionFormat(source.head(), partition)
        val partitionType = partitionCombined._2
        val toDateColumn = partitionCombined._1.replace("$", partition)

        val allColumns = source.columns.filter(col => !col.equals(partition)).mkString(",")

        // read source after change partition to date
        source = spark.sql(f"select $allColumns, $toDateColumn as $partition from ${global.srcSchema}.$trgTable $postfixQuery")

        Utils.writeToBigQuery(source, partition, partitionType, bucketCols, srcTable)
      } else {
        source = spark.sql(f"select * from ${global.srcSchema}.$trgTable $postfixQuery")

        Utils.writeToBigQuery(source, partition, "MONTH", bucketCols, srcTable)
      }


    } else {
      // Partition absence
      val source = spark.sql(f"select * from ${global.srcSchema}.$trgTable $postfixQuery")

      Utils.writeToBigQuery(source, "", "", bucketCols, srcTable)
    }

    spark.sql(f"drop table if exists ${global.srcSchema}.$trgTable")
  }
}
