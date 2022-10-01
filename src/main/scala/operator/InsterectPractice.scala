package operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @Name InsterectPractice
 * @Description TODO
 * @Author joker
 * @Date 2022/10/1 14:14
 * @Version 1.0
 * */
object InsterectPractice {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("CoalescePractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sparkContext.makeRDD(List(3, 7, 6, 23), 2)
    val rdd2: RDD[Int] = sparkContext.makeRDD(List(3, 23, 1, 15), 2)

    val value: RDD[Int] = rdd1.intersection(rdd2)
    value.collect().foreach(println)

    println("---------" * 20)

    rdd1.subtract(rdd2).collect().foreach(println)

    println("---------" * 20)

    rdd1.union(rdd2).collect().foreach(println)

    println("---------" * 20)


    val rdd3: RDD[String] = sparkContext.makeRDD(List("hello", "world", "scala", "spark"), 2)

    rdd1.zip(rdd2).collect().foreach(println)


    println("---------" * 20)

    rdd1.zip(rdd3).collect().foreach(println)

    sparkContext.stop()
  }

}
