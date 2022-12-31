package com.ji.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

import scala.math.Ordered.orderingToOrdered
import scala.math.Ordering.Implicits.infixOrderingOps

case class ProductRating(userId:Int,productId:Int,score:Double,timestamp:Int)
case class MongoConfig(uri:String,db:String)
// 一个用户对应一个推荐列表，一个商品对应一个相似度列表，列表元素为productId和score(预测评分或相似度)
case class Recommendation (productId:Int,score:Double)
case class UserRecs(userId:Int, recs:Seq[Recommendation])
case class ProductRecs(productId:Int, recs:Seq[Recommendation])

object OfflineRecommender {
  // 定义mongoDB中存储的表名，
  val MONGODB_RATING_COLLECTION = "Rating"
  // mongoDB 写回的表名
  val USER_RECS_COLLECTION = "UserRecs"
  val PRODUCT_RECS_COLLECTION = "ProductRecs"
  //用户推荐列表长度
  val USER_MAX_RECOMMENDATION = 20

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

    // 加载数据,因为spark.mllib.ALS算法要求，转换为RDD:(userId,productId,score)
    val ratingRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => (rating.userId,rating.productId,rating.score)
      ).cache()   //缓存到内存


    // TODO:核心计算过程
    // 1.训练LFM隐语义模型模型
    // 1.1 定义训练数据，ALS.train方法要求数据是mllib中的Rating类型
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // 1.2 定义模型训练参数，rank是维度k，iterations迭代次数，lambda正则化系数
    val (rank, iterations, lambda) = (5, 10, 0.01)
    // 1.3 训练数据，得到模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)


    // 2.用model.predict()获得预测评分矩阵，转换为用户推荐列表
    // 提取出所有用户ID和商品ID的数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()
    // 用userRDD和productRDD做笛卡尔积，得到空的userProductsRDD
    val userProducts = userRDD.cartesian(productRDD)

    val predictRating: RDD[Rating] = model.predict(userProducts)

    // 从预测评分矩阵preRating转换为用户推荐列表:UserRecs
    val userRecsDF: DataFrame = predictRating
      .filter(_.rating > 0)
      .map(
        rating => (rating.user, (rating.product, rating.rating)) //rating是数据类型Rating，也是Rating中的一个元素（评分）
      )
      .groupByKey()
      .map {
        case (userId, recs) =>
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    // 保存到mongo
    userRecsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    // 3.利用商品的特征矩阵model.productFeatures:(int,Array(double))，计算商品相似度列表，为后面实时推荐做准备
    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures
      // 为使用jblas矩阵计算模块，将特征矩阵的 Array（Double）转为DoubleMatrix类型
      .map { case (productId, featuresArray) => (productId, new DoubleMatrix(featuresArray)) }

    // 两两配对商品，计算余弦相似度
    val productRecsDF: DataFrame = productFeatures.cartesian(productFeatures)
      //过滤掉自己与自己的笛卡尔积
      .filter {
        case (a, b) => a._1 != b._1
      }
      //计算余弦相似度
      .map {
        case (a, b) =>
          val simScore = cosineSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.4) //过滤相似度小的
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    //保存到mongo
    productRecsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",PRODUCT_RECS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    spark.stop()
  }

  //计算余弦相似度，两矩阵 点积 /（模长 * 模长）
  def cosineSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
    product1.dot(product2) / ( product1.norm2() * product2.norm2() )  // norm2()第二范式，求模长
  }
}
