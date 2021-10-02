package HealthAnalysis

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Encoders



object DrugFrequencyByLocation {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Drug Frequency By Location")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Drug Frequency By Location")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .getOrCreate()
    
        
                   
       import spark.implicits._    
       val dfMedications=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/medications.csv")
                  .option("header",true)
                  .schema("seqn String,noOfMedicines Integer,drugName String")  //  .option("inferSchema",true)
                  .load
                  .filter(x=> !x.isNullAt(2))
                  .rdd
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
//                                   
////(SEQN,DRUGNAME,1)
////(73557,INSULIN,1)
////(73558,GABAPENTIN,1)    
//
//       import spark.implicits._                        
//       val dfMedications=rdd1.toDF("seqn","drugName","frequency")

                       

       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/demographic.csv")
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
      val out = spark.sql("""select drugName, location, count(frequency) as frequency from Drugs 
                                  group by drugName, location 
                                  order by drugName, location, frequency desc""")
//                      .show(10)
                  
                  
                
                    
      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("path","D:/HealthDataSpark/output/DrugFrequencyByLocation")
          .save
          
          
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}