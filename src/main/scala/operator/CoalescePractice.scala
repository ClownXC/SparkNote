package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name CoalescePractice
 * @Description TODO
 * @Author joker
 * @Date 2021/10/1 13:49
 * @Version 1.0
 * */
object CoalescePractice {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("CoalescePractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sparkContext.makeRDD(List(3, 4, 4, 4, 4, 5, 6, 6, 7, 22, 7, 8, 9, 11, 13, 16, 20, 22, 23), 6)

    val coalecseRDD: RDD[Int] = rdd.coalesce(3)
    val coalecseRDD2: RDD[Int] = rdd.coalesce(3, shuffle = true)


    val repartitionRDD: RDD[Int] = rdd.repartition(12)




    coalecseRDD.saveAsTextFile("output")



    sparkContext.stop()

  }
}
