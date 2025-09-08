package Main

import Main.executor.ExecutorFactory
import com.typesafe.config.{Config, ConfigFactory}

object MigrationMain {
  def main(args: Array[String]): Unit = {
    val jobType = args(0)       // schema or table
    val dbType = args(1)        // HIVE or SQL
    val srcSchema = args(2)
    val bigQueryDB = args(3)
    val srcTable = args(4)


    val conf: Config = ConfigFactory.load()
    global.srcSchema = srcSchema
    global.projectId = conf.getString("fixedArgs.projectId")
    global.gcsBucket = conf.getString("fixedArgs.gcsBucket")
    global.BigQueryDB = bigQueryDB
    global.jobType = jobType
    global.sql_user = conf.getString("fixedArgs.sql_user")
    global.sql_password = conf.getString("fixedArgs.sql_password")
    global.configJobType = conf.getString("fixedArgs.configJobType")
    global.sql_user = conf.getString("fixedArgs.sql_user")
    global.sql_jdbc_url = conf.getString("fixedArgs.sql_jdbc_url")
    global.hive_jdbc_url = conf.getString("fixedArgs.hive_jdbc_url")
    global.configSchema = conf.getString("fixedArgs.configSchema")
    global.configTable = conf.getString("fixedArgs.configTable")

     val executor = ExecutorFactory.createExecutor(dbType, srcTable)
     executor.startFlow()

    if (global.globalError) {
      throw new Exception("Check Errors")
    }
  }
}
