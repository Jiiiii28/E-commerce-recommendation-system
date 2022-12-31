package com.ji.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class Product(productId:Int,name:String,imageUrl:String,categories:String,tags:String)
case class MongoConfig(uri:String,db:String)
case class Recommendation (productId:Int,score:Double)
case class ProductRecs(productId:Int, recs:Seq[Recommendation])

// 用ml的TF-IDF算法
object ContentRecommender {
  val MONGO_PRODUCT_COLLECTION = "Product"
  val MONGO_CONTENT_PRODUCT_RECS_COLLECTION = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.db" -> "recommender",
      "mongo.uri" -> "mongodb://hadoop102:27017/recommender"
    )
    // 创建spark config
    val sparkConf:SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    // 加载数据，做预处理
    val productTagsDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(x => (x.productId, x.tags.replace("|", " "))) //ml分词器默认分隔空格
      .toDF("productId", "tags")    // ml用DF，mllib用RDD
      .cache()

    // TODO:用ml的TF-IDF提取商品tags特征向量
    // 1.实例化一个分词器，默认按空格
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 用分词器做转换，得到一个增加一个新列words的DF
    val wordsDataDF: DataFrame = tokenizer.transform(productTagsDF)

    // 2.定义一个HashingTF工具，计算频次(某一个给定的词语在该文档中出现的次数)
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)  //设置分桶数800
    val featurizedDataDF: DataFrame = hashingTF.transform(wordsDataDF)

    // 3.定义一个IDF，计算TF-IDF （某个特征的重要程度）
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练idf模型（计算）
    val idfModel: IDFModel = idf.fit(featurizedDataDF)
    // 得到增加新列features的DF
    val rescaledDataDF: DataFrame = idfModel.transform(featurizedDataDF)

    // 对数据进行转换，得到RDD形式的features
    val productFeatures: RDD[(Int, DoubleMatrix)] = rescaledDataDF
      .map {
        row => (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray) //SparseVector稀疏向量
      }
      .rdd
      .map {
        case (pid, features) => (pid, new DoubleMatrix(features))
      }

    // 商品两两匹配，计算余弦相似度
    val productRecsDF = productFeatures.cartesian(productFeatures)
      .filter {
        case (a, b) => a._1 != b._1
      }
      .map {
        case (a, b) =>
          val simScore = cosineSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (pid, recs) =>
          ProductRecs(pid, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    // 保存到mongo
    productRecsDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGO_CONTENT_PRODUCT_RECS_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  //计算余弦相似度，两矩阵的点积 /（模长 * 模长）
  def cosineSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
    product1.dot(product2) / ( product1.norm2() * product2.norm2() )  // norm2()第二范式，求模长
  }
}
