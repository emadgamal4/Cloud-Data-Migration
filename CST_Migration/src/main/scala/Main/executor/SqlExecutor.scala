package Main.executor

import Main.global
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}
import java.util.Properties

class SqlExecutor(src_table: String, jdbc_url: String, user: String, password: String) extends Executor{

  def startFlow(): Unit = {
    // create spark session
    val spark = Utils.createSparkSession()

    // JDBC connection properties
    val connectionProperties = new Properties()
    connectionProperties.put("user", user)
    connectionProperties.put("password", password)
    connectionProperties.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    if (global.jobType.equalsIgnoreCase("schema")){
      spark.read.jdbc(
        url = jdbc_url,
        table = f"""(SELECT t.name AS TableName FROM sys.tables t INNER JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = '${global.srcSchema}') t""",
        properties = connectionProperties
      ).collect().map(_.getString(0)).foreach(table => {
        try {
          println(f"running : $table")
          SqlExecuteTable.executeTable(spark, jdbc_url, table, connectionProperties)
          println(f"success : $table")
        } catch {
          case e: Throwable => {
            global.globalError = true
            println(f"Failed : $table : ${e.getMessage}")
            val localFilePath = s"file:///spark_Error_HIVE_${global.srcSchema}.text" // Absolute local path
            val conf = new Configuration()
            val fs = FileSystem.get(new java.net.URI("file:///"), conf)
            val output = fs.create(new Path(localFilePath), true)
            val writer = new BufferedWriter(new OutputStreamWriter(output))
            writer.write(s"--- $table --- \n")
            writer.close()
          }
        }
      })
    } else {
      SqlExecuteTable.executeTable(spark, jdbc_url, src_table, connectionProperties)
    }
  }
}
