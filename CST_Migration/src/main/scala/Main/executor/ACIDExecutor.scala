package Main.executor

import Main.global
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}
import java.sql.Connection

class ACIDExecutor(hiveJdbcUrl: String, source_table: String) extends Executor with Serializable  {

  def startFlow(): Unit = {
    val spark = Utils.createSparkSession()
    val configDF = Utils.readConfigTable(spark)

    // create hive connection and spark session
    val conn: Connection = Utils.createHiveJdbcConnection(hiveJdbcUrl)

    if (global.jobType.equalsIgnoreCase("schema")) {
      spark.catalog.listTables(global.srcSchema).collect().foreach( table => {
          try {
            println(f"running : ${table.name}")
            ACIDExecuteTable.executeTable(spark, conn, table.name, configDF)
            println(f"success : ${table.name}")
          } catch {
            case e: Throwable => {
              global.globalError = true

              spark.sql(f"drop table if exists ${global.srcSchema}.${table.name}_bigQuery_temp")

              println(f"Failed : ${table.name} : ${e.getMessage}")
              val localFilePath = s"file:///spark_Error_HIVE_${global.srcSchema}.text" // Absolute local path
              val conf = new Configuration()
              val fs = FileSystem.get(new java.net.URI("file:///"), conf)
              val output = fs.create(new Path(localFilePath), true)
              val writer = new BufferedWriter(new OutputStreamWriter(output))
              writer.write(s"--- ${table.name} --- \n")
              writer.close()



//              val filePath = f"file:///errors/spark_Error_HIVE_${global.srcSchema}.text"
//              val contentToAppend = f"--- ${table.name} : ${e.getMessage} \n"
//
//              val fw = new FileWriter(filePath, true) // true = append mode
//              val bw = new BufferedWriter(fw)
//
//              bw.write(contentToAppend)
//              bw.close()
            }
          }
        }
      )
    } else {
      ACIDExecuteTable.executeTable(spark, conn, source_table, configDF)
    }
  }

}
