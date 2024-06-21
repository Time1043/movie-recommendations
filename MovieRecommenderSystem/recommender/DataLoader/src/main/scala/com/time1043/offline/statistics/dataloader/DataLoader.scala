package com.time1043.offline.statistics.dataloader

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

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
 * tag.csv
 * @param uid        用户ID
 * @param mid        电影ID
 * @param tag        用户对电影的标签
 * @param timestamp  标签时间
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
 * mongoConfig
 * @param uri             MongoDB连接地址
 * @param db              MongoDB数据库名称
 */
case class MongoConfig(uri: String, db: String)

/**
 * ESConfig
 * @param httpHosts       http的主机列表
 * @param transportHosts  transport的主机列表
 * @param index           需要操作的索引
 * @param clustername     集群名称
 */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clusterName: String)

object DataLoader {
  // file path
  val MOVIE_DATA_PATH = "D:\\code2\\java-code\\movie-recommendations\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\code2\\java-code\\movie-recommendations\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\code2\\java-code\\movie-recommendations\\MovieRecommenderSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  // table name
  val MONGO_MOVIE_COLLECTION = "Movie"
  val MONGO_RATING_COLLECTION = "Rating"
  val MONGO_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    /**
     * config 192.168.64.138
     */
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.64.138:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.64.138:9200",
      "es.transportHosts" -> "192.168.64.138:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )


    // spark entrance
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // read data to RDD
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

    // convert RDD to DataFrame
    val movieDF = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim, attr(4).trim,
          attr(5).trim, attr(6).trim, attr(7).trim, attr(8).trim, attr(9).trim)
      }
    ).toDF()
    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    ).toDF()
    val tagDF = tagRDD.map(
      item => {
        val attr = item.split(",")
        Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
      }
    ).toDF()

    // show data
    // movieDF.show()
    // ratingDF.show()
    // tagDF.show()

    // write data to MongoDB
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(movieDF, ratingDF, tagDF)

    // data pre-processing (mid, tag:tag1|tag2|tag3)
    import org.apache.spark.sql.functions._
    val tagDF2 = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")
    val movieWithTagDF = movieDF.join(tagDF2, Seq("mid"), "left")
    movieWithTagDF.show()

    // write data to Elasticsearch (movie + tag) error!!!
    // implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))
    // storeDataInES(movieWithTagDF)

    // close spark session
    spark.stop()
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // create connection
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    mongoClient(mongoConfig.db)(MONGO_MOVIE_COLLECTION).dropCollection() // drop database if exists
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGO_TAG_COLLECTION).dropCollection()

    // write data to MongoDB
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGO_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // create index
    mongoClient(mongoConfig.db)(MONGO_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGO_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1, "mid" -> 1))
    mongoClient(mongoConfig.db)(MONGO_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1, "mid" -> 1))

    // close connection
    mongoClient.close()
  }

  def storeDataInES(movieDF: DataFrame)(implicit esConfig: ESConfig): Unit = {
    // create connection
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clusterName).build()
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    // delete index if exists
    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index))
      .actionGet().isExists
    ) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }
    // write data to Elasticsearch
    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))
    movieDF.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "100ms")
      .option("es.mapping.id", "mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }
}
