import java.io.FileNotFoundException
import scala.io.StdIn.readLine

object Main extends App{
  println("Please Enter Your metadata Directory : ")
  val metadataDir : String = readLine()
  println("Please Enter Your Table Name : ")
  val tableName: String = readLine()
  println("Please Enter Your Data Directory: ")
  val dirPath : String = readLine().concat(s"\\$tableName")
  println("Please Enter Your Logs Data File Path: ")
  val logs_metadata_path = readLine()
  println("Please Enter Your Logs DB Connection Credentials Path: ")
  val dbCredentialsPath = readLine()

    //val metadataDir: String = "D:\\ITI\\Scala\\Project Files\\Metadata"
  // val dirPath: String = s"D:\\ITI\\Scala\\Project Files\\Data\\$tableName"
    try {
      val metadataMap: (Map[String, Map[String, String]], String) = Extract.readMetaDataFile(metadataDir,tableName)
     // val logs_metadata_path = "D:\\ITI\\Scala\\Project Files\\Metadata\\Logs_Metadata.csv"
      //var credentialsPath = "D:\\ITI\\Scala\\Project Files\\Database_Credentials\\DB_Access.csv"
      val logs_metadata_map = Extract.readLogsMetaData(logs_metadata_path)
      Load.createLogsTable(logs_metadata_map)
      Load.createTable(metadataMap._1, metadataMap._2, tableName)
      Transform.transformData(metadataMap._1, Extract.getListOfLines(Extract.getListOfFiles(dirPath,tableName)),tableName)
    }
//    catch {
//      case e: FileNotFoundException => println("Check if your file exists")
//      case e: NoSuchElementException => println("Check if your file has more lines")
//      case _: Throwable => println("Unknown error happened")
//    }


}
