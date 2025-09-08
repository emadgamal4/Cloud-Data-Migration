package Main.executor

import Main.global

object ExecutorFactory {

  def createExecutor(dbType: String, srcTable: String): Executor = {
    dbType.toUpperCase match {
      case "HIVE" => new ACIDExecutor(global.hive_jdbc_url, srcTable)
//      case "HIVE" => new HiveExecutor(srcTable)
      case "SQL" => new SqlExecutor(srcTable, global.sql_jdbc_url, global.sql_user, global.sql_password)
      case _ => throw new Exception(s"un-supported DB Type: $dbType")
    }
  }

}
