package Main.executor

import Main.{DatePattern, global}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, DriverManager}

object Utils {
  var isInitialized: Boolean = false
  private var spark: SparkSession = _
  
  def createSparkSession(): SparkSession = {
    if (!isInitialized) {
      spark = SparkSession.builder()
        .appName("HiveORCToBigQuery")
        //      .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        //      .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "resources/cst-dev-resources-prj-9685f4778c6d.json")
        .enableHiveSupport()
        .getOrCreate()

      isInitialized = true
    }
    spark
  }

  def createHiveJdbcConnection(hive_jdbc_url: String): Connection = {
    Class.forName("com.cloudera.hive.jdbc41.HS2Driver")
    DriverManager.getConnection(hive_jdbc_url)
  }

  def createTmp(hiveConnection: Connection, srcTable: String, trgTable: String): Boolean = {
    val ddlQuery = f"""CREATE TABLE ${global.srcSchema}.$trgTable
                      |STORED AS PARQUET
                      |TBLPROPERTIES (
                      |  'transactional'='false'
                      |)
                      |As select * from ${global.srcSchema}.$srcTable""".stripMargin

    val stmt = hiveConnection.createStatement()
    stmt.execute("SET hive.stats.autogather=false")
    stmt.execute(f"drop table if exists ${global.srcSchema}.$trgTable")
    stmt.execute(ddlQuery)
  }

  def getPartitionAndClustering(spark: SparkSession, srcTable: String): (String, String) = {
    val rows = spark.sql(s"DESCRIBE FORMATTED ${global.srcSchema}.$srcTable").collect()

    var partitions = ""
    var bucketCols = ""

    var mode = "normal"

    rows.foreach { r =>
      val col = r.getString(0).trim
      val typ = Option(r.getString(1)).getOrElse("").trim
      col match {
        case "# Partition Information"    => mode = "partition"
        case "Bucket Columns" => bucketCols = typ.substring(1, typ.length - 1).replace("`", "")
        case _ =>
          mode match {
            case "partition" =>
              if (col.nonEmpty && !col.startsWith("#")) {
                partitions += col + ","
              } else if (col.isEmpty){
                partitions = partitions.dropRight(1)
                mode = "normal"
              }
            case _ =>
          }
      }
    }

    (partitions, bucketCols)
  }

  def getPartitionFormat(sample: Row, partitionColumn: String): (String, String) = {
    val partitionValue = sample.getAs[String](partitionColumn)

    DatePattern.classify(partitionValue)
  }

  def writeToBigQuery(sourceDF: DataFrame, partition: String, partitionType: String, clusterCols: String, tableName: String): Unit = {
    // Create a natively partitioned table
    val sourceWriter = sourceDF.write
      .format("bigquery")
      .option("writeMethod", "indirect") // OR "direct" for small datasets
      // The GCS bucket that temporarily holds the data before it is loaded to BigQuery.
      .option("temporaryGcsBucket", global.gcsBucket)
      // The data is temporarily stored using the Apache Parquet, Apache ORC or Apache Avro formats.
      .option("intermediateFormat", "parquet") // Defult, Faster than AVRO
      .option("parentProject", global.projectId)
      .option("useBignumericType", "true")
      .option("allowFieldAddition", "true")
      .option("allowFieldRelaxation", "true")
      .mode("overwrite")

    if (partition.nonEmpty)
      sourceWriter
        // Specifies the column used for partitioning
        .option("partitionField", partition)
        .option("partitionType", partitionType) // HOUR, DAY, MONTH, YEAR

    if (clusterCols.nonEmpty)
      sourceWriter.option("clusteredFields", clusterCols)
    // datasetName.tableName
    sourceWriter.save(s"${global.BigQueryDB}.$tableName")
  }

  def readConfigTable(spark: SparkSession) : DataFrame = {
    try {
      spark.read
        .format("bigquery")
        .option("parentProject", global.projectId)
        .load(s"${global.configSchema}.${global.configTable}")
        .select("mapping_name", "ctl_ref_offset_type", "ctl_ref_offset_id", "ctl_ref_offset_date")
        .cache()
    } catch {
      case _: Throwable => spark.emptyDataFrame
    }
  }

  def checkTableInConfig(df: DataFrame, tableName: String): String = {
    df.createOrReplaceTempView("config")

    val res = spark.sql(f"""select * from config where lower(mapping_name) like "%%_$tableName" and lower(mapping_name) like "%%_${global.configJobType}_%%" """).collect()

    if (res.length == 1) {
      if (res.head.getAs[String]("ctl_ref_offset_type").equalsIgnoreCase("date")) {
        res.head.getAs[String]("ctl_ref_offset_date")
      } else {
        res.head.getAs[String]("ctl_ref_offset_id")
      }
    }
    null
  }
}
