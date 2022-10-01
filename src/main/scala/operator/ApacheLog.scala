package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name ApacheLog
 * @Description TODO
 * @Author joker
 * @Date 2022/10/1 00:21
 * @Version 1.0
 * */
object ApacheLog {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("ApacheLog")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

//    val logRDD: RDD[String] = sparkContext.textFile("data/apache.log", 3)
    val logRDD: RDD[String] = sparkContext.textFile("data/apache2.log", 3)
    val urlRDD: RDD[String] = logRDD.map(line => {
      println("============================")
      line.split(" " )(6)
    })

    urlRDD.collect().foreach(println)

    println("--------------------" * 10)

    val urlRDD2: RDD[String] = logRDD.mapPartitions(line => {
      println(">>>>>>>>>>>>>>>>  ")
      line.map(_.split(" ")(6))
    })
    urlRDD2.collect().foreach(println)





  }

}
