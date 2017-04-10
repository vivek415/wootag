import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject

import scala.util.Random
import scala.collection.mutable.Map

/**
  * Created by vivkumar on 4/8/17.
  */
object mainScript {
  def functionToCreateContext():StreamingContext = {
    val conf = new SparkConf()
    val sparkContext = new SparkContext(conf)
    new StreamingContext(sparkContext,Seconds(10))
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate("hdfs://localhost:9000/checkpoint",functionToCreateContext)
    val lines = ssc.textFileStream("hdfs://localhost:9000/stream")
    lines.foreachRDD(eachrdd=>{
      println(eachrdd.name)
      val jsonRDD = eachrdd.map(rdd=> {
        try {
          val obj = new JSONObject(rdd)
          ((obj.getInt("starttime"),obj.get("vid").toString),obj)
        } catch {
          case e:Exception => {
            ((0,""),new JSONObject())
          }
        }

      }).filter(r=>{r._1._1!=0}).groupByKey()

      jsonRDD.foreachPartition(part=>{
        part.foreach(line=>{
          val jvalue = line._2
          val sdf = new SimpleDateFormat("yyyyMMddHH");
          val timepartition = sdf.format(new Date(line._1._1));
          val fs = FileSystem.get(new Configuration())
          if(!fs.exists(new Path("hdfs://localhost:9000/wootag/archived=N/timepartition="+timepartition+"/vid="+line._1._2))) {
            val hiveContext = new HiveContext(ssc.sparkContext)
            hiveContext.sql("ALTER TABLE wootag.videoviews ADD IF NOT EXISTS PARTITION (archived=N,timepartition=" + timepartition + ",vid="+line._1._2+")")
          }

          val hiveContext = new HiveContext(ssc.sparkContext)
          val parquetFileName = "wootag-"+ System.currentTimeMillis+"-"+line._1._1+"-"+line._1._2+"-"+Random.alphanumeric.take(8).mkString+".paraq"
          val tableschema = "message wootag.videoviews {optional int64 starttime; optional int64 currenttime; optional int64 videolength; optional binary uid (UTF8);}"
          val paraquetWriter = new CustomeParquetWrite(tableschema,"hdfs://localhost:9000/wootag/archived=N/timepartition="+timepartition+"/vid="+line._1._2,parquetFileName)
          jvalue.foreach(jobj=>{
            val singleLine =  Map[String,String]()
            singleLine ++ List("starttime"->jobj.get("starttime").toString)
            singleLine ++ List("currenttime"->jobj.get("currenttime").toString)
            singleLine ++ List("videolength"->jobj.get("videolength").toString)
            singleLine ++ List("uid"->jobj.get("uid").toString)

            paraquetWriter.writeRecord(singleLine.toMap)
          })
        })
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
