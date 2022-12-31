package com.ji.online

import com.mongodb.casbah.Imports.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoCollection, MongoCursor}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

case class MongoConfig(uri:String,db:String)
// 一个用户对应一个推荐列表，一个商品对应一个相似度列表，列表元素为productId和score(预测评分或相似度)
case class Recommendation (productId:Int,score:Double)
case class UserRecs (userId:Int, recs:Seq[Recommendation])        //用户推荐列表
case class ProductRecs (productId:Int, recs:Seq[Recommendation])  //商品相似度矩阵

// 定义一个连接助手对象，建立 redis和 mongodb的连接
object ConnHelper extends Serializable {
  // 使用懒变量，使用的时候才初始化
  lazy val jedis = new Jedis("hadoop104",4396)
  jedis.auth("jikeyu020428")
  // 只更新一条数据，DF不能处理，用MongoClient
  lazy val mongoClient: MongoClient = MongoClient(MongoClientURI("mongodb://hadoop102:27017/recommender"))
}

object OnlineRecommender {
  //定义常量和表名
  val RATING_COLLECTION = "Rating"
  val STREAM_RECS_COLLECTION = "StreamRecs"
  val PRODUCT_RECS_COLLECTION = "ProductRecs"

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    // 创建 spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    // 创建 spark session
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建 SparkContext 和 SparkStreamingContext
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"),config("mongo.db") )

    // 加载数据: 相似度矩阵，广播出去提高性能
    val simProductsMatrix: collection.Map[Int, Map[Int, Double]] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      // 将ProductRecs转为Map(pid1:Map(pid2:score,...))，先转为二元组在调用collectAsMap方法转为Map
      .map(item =>
        (item.productId, item.recs.map(x => (x.productId, x.score)).toMap)
      )
      .collectAsMap()
    // 定义相似度矩阵的广播变量
    val simProductsMatrixBC = sc.broadcast(simProductsMatrix)

    // 创建Kafka配置参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    // 创建DStream
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
    )
    // 对kafkaStream处理，产生评分流：kafka的数据格式是：userId|productId|score|timestamp
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map { msg =>
      val strings: Array[String] = msg.value().split("\\|")
      (strings(0).toInt, strings(1).toInt, strings(2).toDouble, strings(3).toInt)
    }

    // 核心：定义评分流的处理流程
    // ratingStream中的RDD元素是一段时间内的一组RDD
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case (uid,pid,score,timestamp) =>
          println("rating data coming >>>>>>>>>>>>>> ")
          // TODO:核心流程
          // 1.从redis里取出当前用户的k次最近评分，保存为数组: Array[(pid,score)]
          val userRecentlyRating:Array[(Int,Double)] = getUserRecentlyRatings(MAX_USER_RATING_NUM,uid,ConnHelper.jedis)

          // 2.从相似度矩阵获取当前商品最相似的商品列表，作为备选列表(筛选掉已经评过分的): Array[pid]
          val candidateProducts: Array[Int] = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, pid, uid, simProductsMatrixBC.value)

          // 3.计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表: Array[(pid,score)]
          val streamRecs: Array[(Int, Double)] = computeProductScore(candidateProducts,userRecentlyRating,simProductsMatrixBC.value)

          // 4.把推荐列表保存到mongo
          saveDataToMongoDB(uid,streamRecs)
      }
    }

    // 启动streaming
    ssc.start()
    println("streaming started>>>>>>>>>>>>>>>")
    ssc.awaitTermination()
  }

  // 给定uid，从redis中获取num个uid的最近评分记录，包装成Array[(pid,score)]
  import scala.collection.JavaConversions._ //对java的数据结构List做map()
  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int,Double)] = {
    // 从jedis中用户的评分队列里获取评分数据: key为userId:1001，value为List[pid:score]
    jedis
      .lrange("userId:"+userId.toString, 0, num)
      .map{ item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  // 获取当前商品的相似度列表，并过滤已经评分过的，作为备选列表
  def getTopSimProducts(num:Int,
                        pid:Int,
                        uid:Int,  //要过滤已经该用户已经评过分的商品，所以需要uid和mongoConfig
                        simProductsMatrix: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig)
  :Array[Int] = {

    // 1.从相似度矩阵中拿取当前商品的相似度列表
    val simProductsArray: Array[(Int, Double)] = simProductsMatrix(pid).toArray

    // 2.从mongo的rating表里查找已经评过分的商品
    val ratingCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(RATING_COLLECTION)
    val ratingExist: Array[Int] = ratingCollection
      .find(MongoDBObject("userId" -> uid))
      .toArray
      .map { item => //只需要pid
        item.get("productId").toString.toInt
      }

    // 3.从相似度列表过滤，排序取前num个
    simProductsArray
      .filter( x => ! ratingExist.contains(x._1) )
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1) //只要pid
  }

  // 计算每个备选商品的评分：(与之前k次评分过的商品的相似度 * 评分权重)的平均值 + 正偏移量 - 负偏移量
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProductsMatrix: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]
                         )
  : Array[(Int,Double)] = {

    // 定义一个长度可变数组ArrayBuffer，保存每个备选商品的基础得分(pid,score)
    val scores = scala.collection.mutable.ArrayBuffer[(Int,Double)]()
    // 定义两个Map用于保存每个商品的高分和低分的计数器 pid -> count
    val increMap = scala.collection.mutable.HashMap[Int,Int]()
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    // 遍历每个备选商品，计算和已评分商品的相似度
    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings){  //相当于双重for循环
      // 从相似度矩阵中获取当前备选商品和当前已评分商品的相似度
      val simScore:Double = getProductSimScore( candidateProduct, userRecentlyRating._1, simProductsMatrix )
      if (simScore > 0.4){
        // 按照公式进行加权计算,得到基础评分，并更新两个累加器
        scores += ( (candidateProduct, simScore * userRecentlyRating._2) )  // +=相当于函数，第一个括号是参数列表，第二个括号表示元组
        if (userRecentlyRating._2 > 3) {
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct,0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct,0) + 1
        }
      }
    }

    // 在scores中以pid为key累加基础评分，加减偏移量，得到最终评分
    scores
      .groupBy(_._1)
      .map{
        case (pid,scoreList) =>
          (pid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(pid,1)) - log(decreMap.getOrDefault(pid,1)))
      }
      // 按照得分排序，返回推荐列表
      .toArray
      .sortWith(_._2 > _._2)
  }

  def getProductSimScore(pid1: Int, pid2: Int,
                         simProductsMatrix: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  : Double = {
    simProductsMatrix.get(pid1) match {
      case Some(sims) => sims.get(pid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 自定义log函数，用换底公式把底e换成10
  def log(i: Int): Double = {
    val N = 10
    math.log(i) / math.log(N)
  }

  //将数据保存到MongoDB    userId -> 1,  recs -> 22:4.5|45:3.8
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig):Unit = {
    //到StreamRecs的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS_COLLECTION)
    // 删除旧数据
    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    // 插入新数据
    streamRecsCollection.insert(MongoDBObject(
      "userId" -> userId,
      "recs" -> streamRecs.map( x => MongoDBObject("productId"->x._1,"score"->x._2) )
    ))
  }

}
