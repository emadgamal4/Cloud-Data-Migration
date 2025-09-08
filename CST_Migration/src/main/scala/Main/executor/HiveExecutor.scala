// For parquet files
package Main.executor

class HiveExecutor(srcTable: String) extends Executor{

  def startFlow(): Unit = {
    // create spark session
    val spark = Utils.createSparkSession()

    // extract partition column and clustering
    val combined = Utils.getPartitionAndClustering(spark, srcTable)
    val partition = combined._1  // must be one
    val bucketCols = combined._2

    // read source to get partitionType and ToDate Function
    if (partition != null && partition.nonEmpty) {
      var source = spark.sql(f"select * from $srcTable where $partition is not null limit 1")
      val partitionCombined = Utils.getPartitionFormat(source.head(), partition)
      val partitionType = partitionCombined._2
      val toDateColumn = partitionCombined._1.replace("$", partition)

      val allColumns = source.columns.filter(col => !col.equals(partition)).mkString(",")

      // read source after change partition to date
      source = spark.sql(f"select $allColumns, $toDateColumn as $partition from $srcTable")

      Utils.writeToBigQuery(source, partition, partitionType, bucketCols, srcTable)
    } else {
      // Partition absence
      val source = spark.sql(f"select * from $srcTable")
      Utils.writeToBigQuery(source, "", "", bucketCols, srcTable)
    }
  }

}
