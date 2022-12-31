package com.ji.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
Product数据集：
3982^                                         商品ID
Fuhlen 富勒 M8眩光舞者时尚节能无线鼠标(草绿)^       商品名称
1057,439,736^                                 商品分类ID，不需要
B009EJN4T2^                                   亚马逊ID，不需要
https://images-cn-4._SY300_QL70_.jpg^         商图片url
外设产品|鼠标|电脑/办公^                          商品分类
富勒|鼠标|电子产品|好用|外观漂亮                    商品UGC标签（用户生成的标签）
 */
case class Product(productId:Int,name:String,imageUrl:String,categories:String,tags:String)

/*
Rating数据集：
* 4867,       用户ID
* 457976,     商品ID
* 5.0,        评分 score
* 1395676800  时间戳
* */
case class Rating(userId:Int,productId:Int,score:Double,timestamp:Int)

// mongoDB连接配置
case class MongoConfig(uri:String,db:String)

object DataLoader {
  // 定义数据文件路径
  val PRODUCT_DATA_PATH = "D:\\MyCodeProjects\\IDEA\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\MyCodeProjects\\IDEA\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  // 定义mongoDB中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建 SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // DataFrame,DataSet的很多操作（如toDF）需要 spark.implicits._
    import spark.implicits._

    //加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      val split: Array[String] = item.split("\\^")
      Product(split(0).toInt,split(1).trim,split(4).trim,split(5).trim,split(6).trim)
    } ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map( i => {
      val split = i.split(",")
      Rating(split(0).toInt,split(1).toInt,split(2).toDouble,split(3).toInt)
    }).toDF()

    //隐式对象 可以隐式调用
    implicit val mangoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    storeDataInMongoDB(productDF,ratingDF)

    spark.stop()
  }

  def storeDataInMongoDB(productDF:DataFrame,ratingDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit ={
    // 新建mongodb连接，客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 定义要操作的表(Collection)，可以理解为 db.Product
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    // 如果表以及存在，删掉
    productCollection.drop()
    ratingCollection.drop()

    // 将当前DF数据存入对应的表中
    productDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对表创建索引
    productCollection.createIndex( MongoDBObject("productId" -> 1) )
    ratingCollection.createIndex( MongoDBObject("productId" -> 1) )
    ratingCollection.createIndex( MongoDBObject("userId" -> 1) )

    mongoClient.close()

  }
}
