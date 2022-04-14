import java.time.LocalDateTime

object Transform {

  val casting_to_integer = (intVal: String)=> intVal.toInt
  val check_Positive = (number:Int) => {
    if(number < 0){
      val result = (number).abs
      result
    }
    else{
      number
    }
  }
  val generate_Id = (id: String) => {
   val new_id: Int = id.toInt + 1000
   new_id
 }
  val mask_string = (len:Int) => {
    val sb = new StringBuilder
    val r = new scala.util.Random

    for(i <- 1 to len){
      var char : Char = r.nextPrintableChar
      while(!char.isLetter){
        char = r.nextPrintableChar()
      }
      sb.append(char)
    }

    sb.toString()
  }
  val upper = (str: String) => str.capitalize
  val check_nulls = (str:String) => {
    if(!str.isEmpty){
      Some(str)}
    else{None}
  }

  def transformData(metaData: Map[String, Map[String, String]], list: List[String], tableName:String): List[String] = {
    list.map((line: String) => {
      Extract.logId = Extract.logId+1
      val eventType = s"'Transformations'"
      //println(s"${id},${tableName},${eventType},${today}")
      Load.insertLogsTable(s"${Extract.logId},'${tableName}',${eventType},'${LocalDateTime.now().toString}'")
      val data :String =  ApplyFunctions(metaData, line)
      Load.insert_data(metaData,data,tableName)
      line
    })
  }
  def ApplyFunctions(metaData: Map[String, Map[String, String]], line: String): String = {
    val metadata_keys = metaData.keys.toList
    val values = line.split(",").toList
    values.zipWithIndex.map{
      case (value, i) => {
        var result = value
        check_nulls(result) match {
          case Some(i) => result = i
          case None =>  result = "None"
        }
        if(result == "None"){
          if(metaData(metadata_keys(i))("datatype") == "Integer"){
              result = "0"
          }
        }
        getAppliedFunctions(metaData, i).foreach {
          case "Casting" => result = casting_to_integer(result.replace('\uFEFF',' ').trim).toString
          case "Plus1000" => result = generate_Id(result.replace('\uFEFF',' ').trim).toString
          case "Mask" => result = mask_string(result.length)
          case "Check_Positive" => result = check_Positive(result.replace('\uFEFF',' ').replace("ï»¿","").trim.toInt).toString
          case "Capitalization" => result = upper(result)
          case _ => "Not implemented"
        }
        if(metaData(metadata_keys(i))("datatype") == "varchar(50)"){
          result = s"'$result'"
        }
        result
      }
    }.reduce((l1, l2) => l1 + "," + l2)

  }
  def getAppliedFunctions(metaData: Map[String, Map[String, String]], index: Int): List[String] = {
    val metaData_keys: List[String] = metaData.keys.toList
    metaData(metaData_keys(index))("functions").split('/').toList
  }



}
