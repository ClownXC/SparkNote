package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name FilterPractice
 * @Description TODO
 * @Author joker
 * @Date 2022/10/1 10:32
 * @Version 1.0
 * */
object FilterPractice {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("FilterPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

//    val value: RDD[Int] = sparkContext.makeRDD(List(3, 4, 5, 6, 7, 8, 9), 3)
//    val value2: RDD[String] = sparkContext.makeRDD(List("Hello", "world", "scala", "start", "Helloworld", "word", "stop"), 3)
//    val groupByRDD: RDD[(Int, Iterable[Int])] = value.groupBy(_ % 2)
//    val groupByRDD1: RDD[(Char, Iterable[String])] = value2.groupBy(_.charAt(0))
//
//
//    groupByRDD.collect().foreach(println)
//    groupByRDD1.collect().foreach(println)

    val logRDD: RDD[String] = sparkContext.textFile("data/apache.log")
    // XXxxxxx
//    logRDD.map(_.split(" ")(3)).filter(_ == "17/05/2015:10:05:03")


    val resRDD: RDD[String] = logRDD.filter(line => {
      val time: String = line.split(" ")(3)
      time.startsWith("17/05/2015")
    })


    resRDD.collect().foreach(println)






    sparkContext.stop()

  }

}
