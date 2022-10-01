package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name FlatMapPractice
 * @Description TODO
 * @Author joker
 * @Date 2022/10/1 10:21
 * @Version 1.0
 * */
object FlatMapPractice {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("FlatMapPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)


    val numRDD: RDD[String] = sparkContext.makeRDD(List(
      "HELLO WORLD",
      "HELLO SPARK"
    ))

    val wordRDD: RDD[Object] = numRDD.flatMap(_.split(" "))
    wordRDD.collect().foreach(println)

//    val value: RDD[List[Int]] = sparkContext.makeRDD(List(
//      List(3, 4),
//      3,
//      List(5, 6)
//    ))
//








    sparkContext.stop()

  }

}
