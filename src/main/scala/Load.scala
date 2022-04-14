import Load.connection

import java.sql.{Connection, DriverManager}
import java.time.LocalDateTime

object Load {
  val connection : Connection = connect_to_db()
  def connect_to_db(): Connection ={
    val Map_DB = Extract.readDBCredentials(Main.dbCredentialsPath)
    var driver = "" ; var url =""; var username = "" ; var password = ""
  Map_DB.foreach((f:(Map[String,String],Map[String,String]))=>{
      driver = f._1("driver")
       url = f._1("url")
       username = f._2("username")
       password = f._2("password")

    })
    Class.forName(driver)
    val conn : Connection = {
      DriverManager.getConnection(url, username, password)
    }
    conn

    //val driver = "org.postgresql.Driver"
//    val url = "jdbc:postgresql://localhost:5432/scalaDB"
//    val username = "postgres"
//    val password = "abdol"
  }

  def createLogsTable(metadata: Map[String,String]) = {
    val create_logs_table = new StringBuilder
    create_logs_table.append("CREATE TABLE IF NOT EXISTS LOGS (")
    val metadata_keys = metadata.keys.toList
    metadata_keys.foreach((key:String)=>{
      create_logs_table.append(s"${key} ${metadata(key)} ,")
    })
    create_logs_table.deleteCharAt(create_logs_table.toString().length - 1)
    create_logs_table.append(");")
    connection.createStatement.executeUpdate((create_logs_table.toString()))
  }
  def insertLogsTable(Line:String) = {
    //val logs_metadata_path = "D:\\ITI\\Scala\\Project Files\\Metadata\\Logs_Metadata.csv"
    val metadata =  Extract.readLogsMetaData(Main.logs_metadata_path)
    val insert_logs_query = new StringBuilder
    val map_keys : List[String] = metadata.keys.toList
    insert_logs_query.append("INSERT INTO Logs(")
    map_keys.foreach((key: String) => {
      insert_logs_query.append(s"$key,")
    })
    insert_logs_query.deleteCharAt(insert_logs_query.toString().length-1)
    val line = Line.split(",").toList
    insert_logs_query.append(")VALUES(")
    line.foreach((field: String) => {
      insert_logs_query.append(s"$field,")
    })
    insert_logs_query.deleteCharAt(insert_logs_query.toString().length-1)
    insert_logs_query.append(");")
   // println(insert_logs_query.toString())
    connection.createStatement.executeUpdate(insert_logs_query.toString())
  }
  def createTable (metadata: Map[String,Map[String,String]],append_write:String,tableName:String) = {
    try {
      val eventType = s"'DDL EVENT'"
      val create_query = new StringBuilder
      create_query.append(s"create table if not exists ${tableName}s (")
      val metaData_keys: List[String] = metadata.keys.toList
      metaData_keys.foreach((key: String) => {
        create_query.append(s"${key} ${metadata(key)("datatype")} ,")
      })
      create_query.deleteCharAt(create_query.toString().length - 1)
      create_query.append(");")

      connect_to_db().createStatement.executeUpdate((create_query.toString()))
      insertLogsTable(s"${Extract.logId},'${tableName}',${eventType},'${LocalDateTime.now().toString}'")
      append_write.toLowerCase() match {
        case "write" =>
          val truncate_query = new StringBuilder
          truncate_query.append(s"TRUNCATE TABLE ${tableName}s")
          val result =  connection.createStatement.executeUpdate((truncate_query.toString()))

          if(result == 0){
            Extract.logId = Extract.logId+1
            //println(s"${id},${tableName},${eventType},${today}")
            insertLogsTable(s"${Extract.logId},'${tableName}',${eventType},'${LocalDateTime.now().toString}'")
                        }
      }
    }
  }
  def insert_data(metadata:Map[String,Map[String,String]],Line:String,tableName:String):Unit = {
    try {

      val insert_query = new StringBuilder
      val map_keys : List[String] = metadata.keys.toList
      insert_query.append(s"insert into ${tableName}s(")
      map_keys.foreach((key: String) => {
        insert_query.append(s"${key},")
      })
      insert_query.deleteCharAt(insert_query.toString().length-1)
      //val line = Line.split(",").toList
      insert_query.append(s")values($Line)")
    //  println(insert_query.toString())
      val result = connection.createStatement.executeUpdate(insert_query.toString())
      if(result == 1){
        Extract.logId = Extract.logId+1
        val eventType = s"'DML EVENT ${Extract.logId} '"
        insertLogsTable(s"${Extract.logId},'${tableName}',${eventType},'${LocalDateTime.now().toString}'")
      }
    }


  }

}
