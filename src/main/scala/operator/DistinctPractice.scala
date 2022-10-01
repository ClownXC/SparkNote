package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName DistinctPractice
 * @Description TODO
 * @Author joker
 * @Date 2021/10/1 13:43
 * @Version 1.0
 * */
object DistinctPractice {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("DistinctPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sparkContext.makeRDD(List(3, 4, 4, 4, 4, 5, 6, 6, 7, 22, 7, 8, 9, 11, 13, 16, 20, 22, 23), 3)
    rdd.distinct().collect().foreach(println)


    sparkContext.stop()

  }
}
