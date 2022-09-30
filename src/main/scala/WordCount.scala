import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Name WordCount
 * @Description  Spark 3 word count code (scala)
 * @Author joker
 * @Date 2021/9/30 21:15
 * @Version 1.0
 * */
object WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sparkContext.textFile("data/wordcount.txt")
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

//    val word2Count: Array[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    word2Count.foreach(println)

    sparkContext.stop()

  }

}
