package operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name AggregateByKeyPractice
 * @Description TODO
 * @Author joker
 * @Date 2022/10/1 14:54
 * @Version 1.0
 * */
object AggregateByKeyPractice {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("PartitionByPractice")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val scoreRDD: RDD[(String, Int)] = sparkContext.makeRDD(List(
      ("chen", 90),
      ("chen", 100),
      ("chen", 98),
      ("li", 98),
      ("chen", 60),
      ("chen", 70),
      ("chen", 90),
      ("li", 90),
    ), 2)

    //    val resRDD: RDD[(String, Int)] = scoreRDD.aggregateByKey(0)((x, y) => math.max(x, y), (x, y) => x + y)
    val resRDD: RDD[(String, Int)] = scoreRDD.aggregateByKey(0)(math.max, _ + _)
    resRDD.collect().foreach(println)


    val resRDD2: RDD[(String, Int)] = scoreRDD.aggregateByKey(0)(_ + _, _ + _)
    resRDD2.collect().foreach(println)

    val resRDD3: RDD[(String, Int)] = scoreRDD.foldByKey(0)(_ + _)
    resRDD3.collect().foreach(println)

    //  求平均值

    val aggregateRDD: RDD[(String, (Int, Int))] = scoreRDD.aggregateByKey((0, 0))(
      (x, y) => (x._1 + y, x._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2)
    )
    aggregateRDD.mapValues {
      case (sum, count) => (sum / count)
    }.collect().foreach(println)

    sparkContext.stop()
  }

}
