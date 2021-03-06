package HealthAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode


object DrugFrequencyByLocation {
  
//  configurations for SparkSession.builder()
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Drug Frequency By Location")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .config("spark.sql.shuffle.partitions","10")    // to make the no of partitions to 3 instead of 200 (spark default)
                            .getOrCreate()
    
        
                   
       import spark.implicits._     // to used operations on df like filter etc.
       val dfMedications=spark.read
                  .format("csv")
                  .option("path","inputFiles/medications.csv")
                  .option("header",true)
                  .schema("seqn String,noOfMedicines Integer,drugName String")  //  .option("inferSchema",true)
                  .load
                  .filter(x=> !x.isNullAt(2))    //filter out the data where deugName is missing
                  .rdd  //   converting to rdd so that we can use rdd specific functions like flatMapValues()
                  .map(x=>(x.getString(0),x.getString(2)))              
                  .flatMapValues(x=>x.split(";"))                       //| .flatMap(x=>x._2.split(";").map((x._1,_,1)))
                  .map(x=>(x._1,x._2,1))  //output : (32615,insulin,1)  //| 
                  .toDF("seqn","drugName","frequency")
                  
                  

       
//       val rdd1=spark.sparkContext.textFile("D:/HealthDataSpark/input/medications.csv")
//       
////                                   .map(x=>(x.split(",")(0),(x.split(",")(1),x.split(",")(2).toUpperCase)))
////                                   .map(x=>(x._1,x._2._2))
//                                   
//                                   .map(x=>(x.split(",")(0),x.split(",")(2).toUpperCase))
//                                   .filter(x=> x._2.nonEmpty)
//                                   .flatMapValues(x=>x.split(";"))
//                                   .map(x=>(x._1,x._2,1))  //output : (32615,insulin,1)
////                                   .take(10)
////                                   .foreach(println)
//                                   
//       import spark.implicits._                        
//       val dfMedications=rdd1.toDF("seqn","drugName","frequency")

                       

       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","inputFiles/demographic.csv")
                  .option("header",true)
                  .schema("seqn Integer,familyMember Integer,location Integer")  //  .option("inferSchema",true)
                  .load
//                  .printSchema

                  
       val joinCond=dfDemographic("seqn")===dfMedications("seqn")
       val joinType="inner"                                                                                          
       val join= dfDemographic.join(dfMedications,joinCond,joinType)    // Broadcast by defult
                              .drop(dfDemographic("seqn"))
                              .drop(dfDemographic("familyMember"))
                              .drop(dfMedications("seqn"))
                              

      join.createOrReplaceTempView("Drugs")
      
                         
//      val out = spark.sql("""select drugName, 
//                                 SUM(CASE WHEN (location=1) THEN 1 ELSE 0 END) AS location1, 
//                                 SUM(CASE WHEN (location=2) THEN 1 ELSE 0 END) AS location2, 
//                                 SUM(CASE WHEN (location=3) THEN 1 ELSE 0 END) AS location3, 
//                                 SUM(CASE WHEN (location=4) THEN 1 ELSE 0 END) AS location4,
//                                 SUM(CASE WHEN (location=5) THEN 1 ELSE 0 END) AS location5
//                             from Drugs 
//                             group by drugName
//                             order by drugName desc""")
//                      .show(10)
                  
      
       import org.apache.spark.sql.functions._  // import to use agg() function
      val out = spark.sql("""select drugName, location , frequency
                             from Drugs """)
                             .withColumn("frequency", expr("coalesce(frequency,0)"))
                             .groupBy("drugName")
                             .pivot("location")
                             .agg(sum("frequency"))
                             .withColumnRenamed("1", "location_1")
                             .withColumnRenamed("2", "location_2")
                             .withColumnRenamed("3", "location_3")
                             .withColumnRenamed("4", "location_4")
                             .withColumnRenamed("5", "location_5")
                             
//                      .show(5)
                  
                
                    
      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("header", true)
          .option("path","outputFiles/DrugFrequencyByLocation")
          .save
          
          
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}