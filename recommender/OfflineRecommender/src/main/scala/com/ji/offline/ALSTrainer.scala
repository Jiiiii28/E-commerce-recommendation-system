package com.ji.offline

import breeze.numerics.sqrt
import com.ji.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import spire.compat.{fractional, integral}

// 用RMSE评估模型

object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender"
    )
    // 创建spark config
    val sparkConf:SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加载数据，得到RatingRDD
    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => Rating(rating.userId,rating.productId,rating.score)
      ).cache()   //缓存到内存

    // 将数据切分成训练集和测试集 (八二开)
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD: RDD[Rating] = splits(0)
    val testingRDD: RDD[Rating] = splits(1)

    // 核心：输出最优参数
    adjustALSParams(trainingRDD,testingRDD)

    spark.stop()
  }

  // ALS参数有rank，iterations，lambda
  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
    val result: Array[(Int, Double, Double)] = for(rank <- Array(5,10,20,50); lambda <- Array(1,0.1,0.01))
      yield {
        // 遍历数组中定义的参数取值
        val model = ALS.train(trainData, rank, iterations = 10, lambda)
        // 用RMSE评估模型
        val rmse = getRMSE (model,testData)
        (rank,lambda,rmse)
      }

    result.foreach(println)
    // 输出最优参数组合
    println(result.minBy(_._3))
  }

  // 用RMSE评估模型： ( 1/n * （实际值-预测值）^2 )^(1/2)
  def getRMSE(model: MatrixFactorizationModel, testData: RDD[Rating]): Double ={
    // 构建userProducts
    val userProducts = testData.map( x => (x.user,x.product) )

    // 得到userProducts预测评分矩阵:(uid,pid,rating)
    val predictRating: RDD[Rating] = model.predict(userProducts)

    // 计算RMSE
    // 1.得到实际和预测的 ((uid,pid),rating)
    val observed: RDD[((Int, Int), Double)] = testData.map(x => ((x.user, x.product), x.rating))
    val predict: RDD[((Int, Int), Double)] = predictRating.map(x => ((x.user, x.product), x.rating))

    // 2.以(uid,pid)为key做内连接
    val joinRDD: RDD[((Int, Int), (Double, Double))] = observed.join(predict)

    // 3.按照公式计算RMSE
    sqrt(
      joinRDD
        .map{
        case ((uid,pid),(observe,predict)) =>
          val err = observe - predict
          err * err
      }.mean()
    )
  }
}
