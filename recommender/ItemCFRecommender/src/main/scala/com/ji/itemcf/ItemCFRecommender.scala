package com.ji.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ProductRating(userId:Int,productId:Int,score:Double,timestamp:Int)
case class MongoConfig(uri:String,db:String)

case class Recommendation (productId:Int,score:Double)
case class ProductRecs(productId:Int, recs:Seq[Recommendation])

object ItemCFRecommender {
  val MONGO_RATING_COLLECTION = "Rating"
  val MONGO_ITEM_CF_PRODUCT_RECS_COLLECTION = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender"
    )
    // 创建spark config
    val sparkConf:SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加载数据，转为DF
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(x => (x.userId, x.productId, x.score))
      .toDF("userId", "productId", "score")
      .cache()

    // TODO:用同现相似度计算商品的相似列表( 给两个商品都打过分的人数 / （商品1打分人数 * 商品2打分人数）^（1/2） )
    // 统计每个商品的评分个数，按pid做groupBy再count，得到（pid,count）
    val productRatingCountDF: DataFrame = ratingDF.groupBy("productId").count()

    // 在原有的评分表Rating上添加count列
    val ratingWithCountDF: DataFrame = ratingDF.join(productRatingCountDF,"productId")  //按pid列join

    // 计算交集，将评分按uid两两配对join，统计被两个商品被同一个用户评过分的次数groupBy
    // 得到 uid,pid1,count1,pid2,count2,
    val joinedDF: DataFrame = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId","product1","score1","count1","product2","score2","count2") //改DF列名
      .select("userId","product1","count1","product2","count2")
    //|userId|product1|count1|product2|count2|
    //+------+--------+------+--------+------+
    //|   496|  379243|   423|  379243|   423|
    //|   496|  379243|   423|   48907|   325|
    //|   496|  379243|   423|  286997|  3355|

    // 创建临时表，用于sql查询
    joinedDF.createOrReplaceTempView("joined")

    // 按pid1，pid2做groupBy，统计出uid的数量，就是给两个商品都打过分的人数 ； sql中的first()函数可以保留groupBy后其他列的值!!!
    val cocurrenceDF: DataFrame = spark.sql(
      """
        |select product1, product2, count(userId) as cocount, first(count1) as count1, first(count2) as count2
        |from joined
        |group by product1,product2
        |""".stripMargin
    ).cache()
   //|product1|product2|cocount|count1|count2|
    //+--------+--------+-------+------+------+
    //|  352021|  438242|     19|   258|   321|
    //|  460360|    8195|     77|   343|  1398|


    val simDF: DataFrame = cocurrenceDF
      // 提取需要的数据，包装成RDD（pid1，pid2，同现相似度score）
      .map {
        row =>
          val coocSim = coocurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
          ( row.getInt(0), (row.getInt(1), coocSim) )
      }
      .rdd
      // 过滤掉pid相同的一行数据，包装成ProductRecs类型
      .groupByKey()
      .map {
        case (pid, recs) => ProductRecs(
          pid,
          recs.toList.filter(x => pid != x._1).sortWith(_._2 > _._2).take(MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2))
        )
      }
      .toDF()

    // 保存到mongo
    simDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGO_ITEM_CF_PRODUCT_RECS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // 计算同现相似度
  def coocurrenceSim(cocount: Long, count1: Long, count2: Long):Double = {
    cocount / math.sqrt( count1 * count2 )
  }

}
