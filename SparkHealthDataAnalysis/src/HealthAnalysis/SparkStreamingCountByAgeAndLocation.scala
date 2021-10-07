

package HealthAnalysis

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.ForeachWriter
import java.io.FileWriter
import org.apache.spark.sql.Row
import org.apache.commons.io.FileUtils
import java.io.File

object SparkStreamingCountByAgeAndLocation {
  
  def main(args : Array[String]){
    
    
      def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "SparkStreamingCountByAgeAndLocation")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
      
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("SparkStreamingCountByAgeAndLocation")
      
      val spark=SparkSession.builder()                      
//                            .config(sparkConf)
                              .master("local[*]")
                              .appName("SparkStreamingCountByAgeAndLocation")
                            .config("spark.sql.shuffle.partitions","3")    // to make the no of partitions to 3 instead of 200 (spark default)
                            .getOrCreate()
    
//      val s=spark.read
                            
      var s=spark.readStream
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/streaming")
                  .option("header",true)
                  .schema("seqn Integer, familyMember Integer, location Integer, income Integer, insuranceAmount Double, age Integer")  //  .option("inferSchema",true)
                  .load
                  .drop("familyMember", "income", "insuranceAmount")
//                  .printSchema
                  
       import org.apache.spark.sql.functions._  // import to use expr() in withColumn() function
        s=s.withColumn("timestamp", expr("now()")) 
                  .withWatermark("timestamp", "1 minutes") // saves the output only after this time

      s.createOrReplaceTempView("table")
       
//      val x=spark.sql("select age, location, count(*) noOfMenbers from table group by age,location having noOfMenbers>20 order by age desc,location desc")
//                 .show(30)


      val x=spark.sql("""SELECT  age , 
                                 SUM(CASE WHEN (location=1) THEN 1 ELSE 0 END) AS location1, 
                                 SUM(CASE WHEN (location=2) THEN 1 ELSE 0 END) AS location2, 
                                 SUM(CASE WHEN (location=3) THEN 1 ELSE 0 END) AS location3, 
                                 SUM(CASE WHEN (location=4) THEN 1 ELSE 0 END) AS location4,
                                 SUM(CASE WHEN (location=5) THEN 1 ELSE 0 END) AS location5
                         from table  group by age""")
//                 .show(100)
      
      
      
                         
                         
                         
                         
                         
                         
                         
                         
 val writerForText = new ForeachWriter[Row] {
    var fileWriter: FileWriter = _

   

    override def open(partitionId: Long, version: Long): Boolean = {
      FileUtils.forceMkdir(new File(s"D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation/${partitionId}"))
      fileWriter = new FileWriter(new File(s"D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation/${partitionId}/temp.csv"))
//      fileR = new FileReader(new File(s"D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation/${partitionId}/temp.csv"))
      
      true
    }
    
    override def process(value: Row): Unit = {
      print(value.toSeq.mkString(",")+"\n")
      fileWriter.append(value.toSeq.mkString(",")).append("\n")
//      fileWriter.append(value.toSeq.mkString(","))
    }

    override def close(errorOrNull: Throwable): Unit = {
      fileWriter.close()
    }
  }

val query = x.writeStream
//          .format("csv")
//          .option("path", "D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation")
//          .outputMode("complete")  // both modes working fine
          .outputMode("update")
          .option("header", true)
          .option("checkpointLocation","D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation/checkPoint")
          .foreach(writerForText)
          .start()
          .awaitTermination()
      
          // file sink only supports append mode, and append mode not supported with aggregations in query
//      val out=x.writeStream
////          .format("console")
//          .format("csv")
//          .option("path", "D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation")
//          .outputMode("complete")
//          .option("header", true)
//          .option("checkpointLocation","D:/HealthDataSpark/output/SparkStreamingCountByAgeAndLocation/checkPoint")
//          .start()
//          .awaitTermination()
//      println("transfered")
          
  }
}