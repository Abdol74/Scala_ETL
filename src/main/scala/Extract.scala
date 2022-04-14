import java.io.File
import java.time.LocalDateTime
import scala.io.Source
import scala.io.StdIn.readLine

object Extract {

  var logId  : Int = 0
  def getListOfFiles(dir: String,tableName:String): List[File] = {
    val new_dir = new File(dir)
    val list_Files: List[File] = new_dir.listFiles().
      filter((f: File) => {
        f.isFile && f.getName.endsWith(".csv")
      }).toList
    list_Files.map((file:File)=>{
      logId = logId+1
      val eventType = s"'ReadFromFile${file.getName}'"
      //println(s"${id},${tableName},${eventType},${today}")
      Load.insertLogsTable(s"${logId},'${tableName}',${eventType},'${LocalDateTime.now().toString}'")

    })
    list_Files
  }
  def readMetaDataFile(dir: String,tableName:String): (Map[String, Map[String, String]],String)  = {
    val new_Dir = new File(dir)
    var appendOrWrite : String = ""
    var metadata: Map[String, Map[String, String]] = Map()
    val list_Files: List[File] = new_Dir.listFiles().filter((f: File) => {
      f.isFile && f.getName.endsWith(".csv") && f.getName.contains(s"${tableName}_Metadata")
    }).toList
    list_Files.foreach((f: File) => {
      logId = logId+1
      val eventType = s"'ReadFromFile${f.getName}'"
      //println(s"${id},${tableName},${eventType},${today}")
      Load.insertLogsTable(s"${logId},'${tableName}',${eventType},'${LocalDateTime.now().toString}'")
      val dataBeforeDrop : List[String] = Source.fromFile(f.getPath).getLines().take(1).toList
      val dummyList = dataBeforeDrop.map((line:String)=>{
        val len = dataBeforeDrop.length
        val values = line.split(",")
        values(values.length-1)
      })
      appendOrWrite = dummyList(0)



      val data = Source.fromFile(f.getPath).getLines().drop(2).toList
      metadata = data.map((line: String) => {
        val value = line.split(",")
        (value(0), Map(("functions" -> (value(2))), ("datatype" -> (value(1)))))
      }).toMap
    })
    (metadata,appendOrWrite)
  }
  def readLogsMetaData(metadataPath:String):Map[String,String] = {
    val bufferedSource =Source.fromFile(metadataPath)
    val lines =bufferedSource.getLines().drop(2).toList
    lines.map((line:String)=>{
      val value = line.split(",")
      (value(0),value(1))
    }).toMap
  }
  def getListOfLines(files: List[File]): List[String] = {
    files.map((f: File) => {
      val myLines = Source.fromFile(f.getPath).getLines().toList
      myLines
    }).reduce((l1, l2) => l1 ++ l2)
  }

  def readDBCredentials(credentialsPath:String):Map[Map[String,String],Map[String,String]] = {
    val bufferedSource =Source.fromFile(credentialsPath)
   val lines =  bufferedSource.getLines().toList
    lines.map((line:String)=>{
      val values = line.split(',')
      (Map("driver"->values(0),"url"->values(1)),Map("username"->values(2),"password"->values(3)))
    }).toMap

  }

}
