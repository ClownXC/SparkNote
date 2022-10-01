package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name TransformPractice
 * @Description TODO
 * @Author joker
 * @Date 2022/10/1 00:06
 * @Version 1.0
 * */
object MapPractice {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val seq = Seq(1, 2, 3, 4, 5, 6)
    val fileRDD: RDD[Int] = sparkContext.makeRDD(seq, 3)

//    val value: RDD[Int] = fileRDD.map((num: Int) => { num * 2 })
//    val value: RDD[Int] = fileRDD.map((num) => {num * 2})
//    val value: RDD[Int] = fileRDD.map(num => {num * 2})
//    val value: RDD[Int] = fileRDD.map(num => num * 2)
    val value: RDD[Int] = fileRDD.map(_ * 2)



    value.collect().foreach(println)

    sparkContext.stop()
  }

}
