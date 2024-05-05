package com.time1043.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date


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
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

/**
 * mongoConfig
 * @param uri             MongoDB连接地址
 * @param db              MongoDB数据库名称
 */
case class MongoConfig(uri: String, db: String)

/**
 * 推荐电影
 * @param mid      电影推荐的id
 * @param score    电影推荐的评分
 */
case class Recommendation(mid: Int, score: Double)

/**
 * 电影类别推荐
 * @param genres    电影类别
 * @param recs      top10的电影集合
 */
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {
  // table name (reading)
  val MONGO_MOVIE_COLLECTION = "Movie"
  val MONGO_RATING_COLLECTION = "Rating"

  // table name (writing for statistics)
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  val MOST_SCORE_OF_NUMBER = 10

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
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // read data from mongo
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    // create temporary view for rating and movie
    ratingDF.createOrReplaceTempView("rating")
    movieDF.createOrReplaceTempView("movie")


    // statistics (writing to mongo)
    /**
     * 历史热门电影统计 (评分次数最多的电影)
     *
     * mid, count
     */
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from rating group by mid order by count desc")
    storeDFInMongo(rateMoreMoviesDF, RATE_MORE_MOVIES)


    /**
     * 近期热门电影统计 (按照时间排序，年月分组，评分次数最多的电影)
     */
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearMonth from rating")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfYearMonth")

    val rateMoreRecentlyMoviesDF = spark.sql(
      """
        |select mid, count(mid) as count, yearMonth
        |from ratingOfYearMonth
        |group by yearMonth, mid
        |order by yearMonth desc, count desc
        |""".stripMargin)
    storeDFInMongo(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)


    /**
     * 电影平均评分统计 (所有电影的平均评分)
     *
     * mid, avgScore
     */
    val averageMoviesDF = spark.sql("select mid, avg(score) as avgScore from rating group by mid")
    storeDFInMongo(averageMoviesDF, AVERAGE_MOVIES)


    /**
     * 电影类别top10推荐 (按照电影类型分组，每个类别top10的电影)
     */
    // add column "avgScore" to movieDF (inner join)
    val movieWithScore = movieDF.join(averageMoviesDF, Seq("mid"))

    // enumerate all genres
    val genres = List(
      "Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy",
      "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western"
    )
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // Cartesian product
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter { // filter out movies not in the genre list
        case (genres, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
      .map { // reduce data volume
        case (genres, movieRow) => (genres, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avgScore")))
      }.groupByKey() // group by genres
      .map {
        case (genres, items) => GenresRecommendation(genres, items.toList.sortWith(_._2 > _._2)
          .take(MOST_SCORE_OF_NUMBER).map(item => Recommendation(item._1, item._2)))
      }.toDF()

    storeDFInMongo(genresTopMoviesDF, GENRES_TOP_MOVIES)


    spark.stop() // end
  }

  def storeDFInMongo(dataFrame: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    dataFrame.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
