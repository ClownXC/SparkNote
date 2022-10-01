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

    val urlRDD2: RDD[String] = logRDD.mapPartitions(partition => {
      println(">>>>>>>>>>>>>>>>  ")
      partition.map(_.split(" ")(6))
    })
    urlRDD2.collect().foreach(println)

    println("--------------------" * 10)

    val urlRDD3: RDD[String] =logRDD.mapPartitionsWithIndex((index, partition) => {
      if(index == 1){
        partition
      }else{
        Nil.iterator
      }
    })
    urlRDD3.collect().foreach(println)


    val urlRDD5: RDD[(Int, String)] = logRDD.mapPartitionsWithIndex((index, partition) => {
      partition.map(line => (index, line.split(" ")(3)))
    })
    urlRDD5.collect().foreach(println)





  }

}
