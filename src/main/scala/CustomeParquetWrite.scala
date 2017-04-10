import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import parquet.example.data.simple.SimpleGroup
import parquet.example.data.{Group, GroupWriter}
import parquet.hadoop.ParquetWriter
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.io.api.RecordConsumer
import parquet.schema.{MessageType, MessageTypeParser}

/**
  * Created by vivkumar on 4/9/17.
  */

class CustomeGroupWriteSupport(schema: MessageType) extends WriteSupport[Group] {
  var groupWriter: GroupWriter = null
  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    groupWriter = new GroupWriter(recordConsumer, schema)
  }
  override def init(configuration: Configuration): WriteContext = {
    new WriteContext(schema, new java.util.HashMap[String, String]())
  }
  override def write(record: Group) {
    groupWriter.write(record)
  }
}

class CustomeParquetWrite(schemaStr: String, pathDir: String, pathFile: String) {
  val schema: MessageType = MessageTypeParser.parseMessageType(schemaStr)
  val pathFinal: Path = findWritableFilePath(pathDir, pathFile)
  val tempPath:Path = findWritableTempPath(pathFinal)

  val schemaColumns = schema.getColumns
  val schemaTypes = (for(idx <- 0 until schemaColumns.size) yield schemaColumns.get(idx).getType).toSeq
  val schemaVsIDX = (for(idx <- 0 until schemaColumns.size ; if schemaColumns.get(idx).getPath.size == 1) yield { schemaColumns.get(idx).getPath.apply(0) -> (idx, schemaColumns.get(idx).getType)}).toMap

  val writeSupport = new CustomeGroupWriteSupport(schema)

  val writer = new ParquetWriter[Group](tempPath, writeSupport, parquet.hadoop.metadata.CompressionCodecName.SNAPPY, 32 * 1024 * 1024, 128 * 1024, 16 * 1024, true, false)


  def close() = {
    writer.close()
    val fsForPath = tempPath.getFileSystem(new Configuration)
    fsForPath.rename(tempPath, pathFinal)
    fsForPath.close()
  }

  def writeRecord(entries: Map[String, String]) = {
    val record = new SimpleGroup(schema)
    var currsetdatagroup:parquet.example.data.Group = null
    var currunknowngroup:parquet.example.data.Group = null
    for(eachKeyVal <- entries){
      //println(eachKeyVal)
      if (eachKeyVal._1.startsWith("_cd_")) {
        if (currsetdatagroup == null)
          currsetdatagroup = record.addGroup("setdata")
        val currgroup = currsetdatagroup.addGroup("map")
        currgroup.append("key", eachKeyVal._1)
        currgroup.append("value", eachKeyVal._2)
      }else if (schemaVsIDX.contains(eachKeyVal._1)){
        val reqidx = schemaVsIDX(eachKeyVal._1)
        reqidx._2 match {
          case parquet.schema.PrimitiveType.PrimitiveTypeName.INT64 => record.add(reqidx._1, eachKeyVal._2.toLong)
          case parquet.schema.PrimitiveType.PrimitiveTypeName.INT32 => record.add(reqidx._1, eachKeyVal._2.toInt)
          case parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN => record.add(reqidx._1, eachKeyVal._2.toBoolean)
          case parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY => record.add(reqidx._1, eachKeyVal._2)
          case parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT => record.add(reqidx._1, eachKeyVal._2.toFloat)
          case parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE => record.add(reqidx._1, eachKeyVal._2.toDouble)
          case _ => record.add(reqidx._1, eachKeyVal._2)
        }
      }else {
        if (currunknowngroup == null)
          currunknowngroup = record.addGroup("unknown")
        val currgroup = currunknowngroup.addGroup("map")
        currgroup.append("key", eachKeyVal._1)
        currgroup.append("value", eachKeyVal._2)
      }
    }
    writer.write(record)
  }

  def findWritableTempPath(actualPath:Path): Path = {
    val actualPathUri = actualPath.toUri
    val justScheme = actualPathUri.getScheme
    val justAuthority = actualPathUri.getAuthority
    val justFile = actualPath.getName
    val justPath = actualPathUri.getRawPath.stripSuffix(justFile)
    val writableTempDir = new Path(justScheme, justAuthority, "/scratch" + justPath)
    val writableTempPath = new Path(justScheme, justAuthority, "/scratch" + justPath + "/" + justFile)
    val fs = writableTempPath.getFileSystem(new Configuration())
    if (!fs.exists(writableTempDir))
      fs.mkdirs(writableTempDir)
    fs.close()
    return writableTempPath
  }

  def findWritableFilePath(pathDirA:String, pathFileA: String) : Path = {
    val job = new Job()
    val dirpath:Path = new Path(pathDirA)
    val fs = dirpath.getFileSystem(job.getConfiguration())
    if (!fs.exists(dirpath))
      fs.mkdirs(dirpath)
    var i = 0
    var finalpath = new Path(dirpath, new Path(pathFileA+".parquet"))
    while (fs.exists(finalpath)){
      i +=1
      finalpath = new Path(dirpath, new Path(pathFileA + "_"+i+".parquet"))
    }
    fs.close()
    return finalpath
  }
}
