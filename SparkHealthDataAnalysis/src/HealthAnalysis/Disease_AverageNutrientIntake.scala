package HealthAnalysis

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode


object Disease_AverageNutrientIntake {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "Disease and Average Nutrient Intake")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("Disease and Average Nutrient Intake")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .getOrCreate()
    
        
       
       val rdd1=spark.sparkContext.textFile("D:/HealthDataSpark/input/medications.csv")
                                   .map(x=>(x.split(",")(0),x.split(",")(1).toUpperCase))
                                   .filter(x=> x._2.nonEmpty)
                                   .flatMapValues(x=>x.split(";"))
//                                   .take(10)
//                                   .foreach(println)
                                   
                         

       import spark.implicits._                        
       val dfMedications=rdd1.toDF("seqn","drugName")

                       

       val dfDemographic=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/diet.csv")
                  .option("header",true)
                  .schema("seqn String,carbohyderates Double,sugar Double,calcium Double, waterIntake Double")  //  .option("inferSchema",true)
                  .load
//                  .printSchema

                  
//       spark.sql("SET spark.sql.autoBroadcastJoinThreshold=-1")
       
       val joinCond=dfDemographic("seqn")===dfMedications("seqn")
       val joinType="inner"                                                                                          
       val join= dfDemographic.join((dfMedications),joinCond,joinType)
                              .drop(dfDemographic("seqn"))
                              .drop(dfMedications("seqn"))
                              

      join.createOrReplaceTempView("Drugs")
      val out = spark.sql("""select drugName DrugName, avg(carbohyderates) as AverageCarbohyderates, 
                                  avg(sugar) as AverageSugar,
                                  avg(calcium) as AverageCalcium, 
                                  avg(waterIntake) as AverageWaterIntake 
                                  from Drugs 
                                  group by drugName 
                                  order by drugName asc""")
//                      .show(10)
                  
                  
                
                    
      out.write
          .format("csv")
          .mode(SaveMode.Overwrite)
          .option("path","D:/HealthDataSpark/output/Disease_AverageNutrientIntake")
          .save
//      scala.io.StdIn.readLine()
      spark.close()
  }
  
}