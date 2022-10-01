package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name GlomPractice
 * @Description TODO
 * @Author joker
 * @Date 2021/10/1 10:32
 * @Version 1.0
 * */
object GlomPractice {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("GlomPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val value: RDD[Int] = sparkContext.makeRDD(List(3, 4, 5, 6, 7, 8, 9), 3)
    val glomRDD: RDD[Array[Int]] = value.glom()
    glomRDD.collect().foreach(data => println(data.mkString(", ")))


    val sum: Int = glomRDD.map(_.max).collect().sum
    println("分区最大值之和: " + sum)


    sparkContext.stop()

  }

}
