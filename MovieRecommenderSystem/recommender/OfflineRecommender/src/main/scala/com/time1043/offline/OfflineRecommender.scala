package com.time1043.offline

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.jblas.DoubleMatrix


/**
 * movie.csv
 * @param mid         电影ID
 * @param name        电影名称
 * @param description 电影描述
 * @param duration    电影时长
 * @param issue       发行日期
 * @param shoot       拍摄日期
 * @param language    语言
 * @param genres      电影类型
 * @param actors      演员
 * @param directors   导演
 */
case class Movie(mid: Int, name: String, description: String, duration: String, issue: String, shoot: String,
                 language: String, genres: String, actors: String, directors: String)

/**
 * rating.csv
 * @param uid        用户ID
 * @param mid        电影ID
 * @param score      用户对电影的评分
 * @param timestamp  评分时间
 */
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
 * mongoConfig
 * @param uri             MongoDB连接地址
 * @param db              MongoDB数据库名称
 */
case class MongoConfig(uri: String, db: String)

/**
 * 标准推荐
 * @param mid
 * @param score
 */
case class Recommendation(mid: Int, score: Double)

/**
 * 用户推荐
 * @param uid
 * @param recs
 */
case class UserRecs(uid: Int, recs: Seq[Recommendation])

/**
 * 电影相似度 电影推荐
 * @param mid
 * @param recs
 */
case class MovieRecs(mid: Int, recs: Seq[Recommendation])


object OfflineRecommender {

  // table name (reading)
  val MONGO_MOVIE_COLLECTION = "Movie"
  val MONGO_RATING_COLLECTION = "Rating"

  // table name (writing for offline or online recommender)
  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 20


  def main(args: Array[String]): Unit = {

    /**
     * config 192.168.64.138
     */
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.64.138:27017/recommender",
      "mongo.db" -> "recommender"
    )


    // spark entrance
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Offline")
      .set("spark.executor.memory", "4G").set("spark.driver.memory", "2G")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    // read data from mongo
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)).cache()

    val movieRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .rdd
      .map(_.mid).cache()

    val userRDD = ratingRDD.map(_._1).distinct()


    /**
     * train model
     */
    // dataset and parameters
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    val (rank, iterations, lambda) = (50, 5, 0.1)
    // ALS model
    val model = ALS.train(trainData, rank, iterations, lambda)

    // user recommendation matrix
    val userMovies = userRDD.cartesian(movieRDD) // matrix
    val preRatings = model.predict(userMovies) // predict

    val userRecs = preRatings
      .filter(x => x.rating > 0.6)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2) // 降序排序
          .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()

    // 写入mongoDB数据库
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    /**
     * 计算电影的相似度矩阵以备实时推荐使用
     * 得到电影矩阵 (n x k)
     */

    // 计算电影的特征矩阵
    val movieFeatures = model.productFeatures.map {
      case (mid, features) => (mid, new DoubleMatrix(features))
    }
    // 过滤
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter {
        case (x, y) => x._1 != y._1
      }
      .map {
        case (x, y) =>
          val simScore = consinSim(x._2, y._2)
          (x._1, (y._1, simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map {
        case (mid, item) => MovieRecs(mid, item.toList.map(x => Recommendation(x._1, x._2)))
      }.toDF()

    // 写入数据库
    movieRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def consinSim(x: DoubleMatrix, y: DoubleMatrix): Double = {
    x.dot(y) / (x.norm2() * y.norm2())
  }

}
