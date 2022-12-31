package com.ji.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

case class Rating(userId:Int,productId:Int,score:Double,timestamp:Int)
case class MongoConfig(uri:String,db:String)

object StatisticsRecommender {
  // 定义mongoDB中存储的表名，
  val MONGODB_RATING_COLLECTION = "Rating"
  // mongoDB 写回的表名
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender"
    )
    // 创建spark config
    val sparkConf:SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 从mongo加载数据，转换成DF
    val ratingDF = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    // 创建临时视图
    ratingDF.createOrReplaceTempView("ratings")

    // 通过临时视图ratings用sparkSql做不同的统计推荐 写入到mongo
    // 1.历史热门商品，按评分个数降序
    val rateMoreProductsDF = spark.sql("select productId,count(*) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)

    // 2.近期热门商品，把时间戳转换成yyyyMM格式统计
    // 2.1创建一个日期的格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    // 2.2注册UDF，将timestamp转换为年月格式yyyyMM (匿名函数)
    //原本的数据x是Int秒数，Date()函数需要Long毫秒数，用x*1000L转换；format得到的是String，后面需要排序，转换成Int
    spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    // 2.3对原始数据sql 两步
    val ratingOfYearMonthDF = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyProductsDF = spark.sql("select productId,count(*) as count,yearmonth from ratingOfMonth " +
      "group by yearmonth,productId order by yearmonth desc,count desc")

    // 2.4 保存到mongo
    storeDFInMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)


    // 3.优质商品，平均评分
    val avgProductsDF = spark.sql("select productId,avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(avgProductsDF,AVERAGE_PRODUCTS)


    spark.stop()
  }

  // 将DF写入到 mongo
  def storeDFInMongoDB(df: DataFrame, collectionName: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collectionName)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
