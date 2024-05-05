package com.time1043

/**
 * Movie数据
 *
 * 260                                                                                          电影ID mid
 * Star Wars: Episode IV - A New Hope (1977)                                                    电影名称 name
 * Princess Leia is captured and held hostage by the evil Imperial forces in their effort ...   电影描述 description
 * 121 minutes                                                                                  电影时长 duration
 * September 21, 2004 (USA)                                                                     发行时间 issue
 * 1977                                                                                         拍摄时间 shoot
 * English                                                                                      电影语言 language
 * Action",Adventure,Sci-Fi                                                                     电影类型 genres
 * Mark Hamill,Harrison Ford,Carrie Fisher,Peter Cushing,Alec Guinness,Anthony Daniels,...      电影导演 directors
 * George Lucas                                                                                 电影演员 actors
 */
case class Movie(mid: Int, name: String, description: String,
                 duration: String, issue: String, shoot: String,
                 language: String, genres: String,
                 directors: String, actors: String)

/**
 * Rating数据
 *
 * 1                                                                                            用户ID uid
 * 31                                                                                           电影ID mid
 * 2.5                                                                                          用户对电影的评分 score
 * 1260759144                                                                                   评分时间 timestamp
 */
case class Rating(uid: Int, mid: Int, score: Double, timestamp: Long)

/**
 * Tag数据
 *
 * 15                                                                                           用户ID uid
 * 1955                                                                                         电影ID mid
 * dentist                                                                                      标签名称 tag
 * 1193435061                                                                                   标签创建时间 timestamp
 */
case class Tag(uid: Int, mid: Int, tag: String, timestamp: Long)

/**
 * config for MongoDB
 * @param uri               MongoDB URI
 * @param db                database name
 */
case class MongoConfig(uri: String, db: String)

/**
 * config for Elasticsearch
 * @param httpHost         http host
 * @param transportHost    transport host
 * @param index            index name
 * @param clusterName      default is "elasticsearch"
 */
case class ElasticConfig(httpHost: String, transportHost: String, index: String, clusterName: String)


object DataLoader {
  def main(args: Array[String]): Unit = {
    // config
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "elastic.httpHost" -> "localhost:9200",
      "elastic.transportHost" -> "localhost:9300",
      "elastic.index" -> "recommender",
      "elastic.clusterName" -> "elasticsearch"
    )

    // spark entrance
    val sparkConf = null
    val sparkSession = null

    // read data
    val movieRDD = null
    val ratingRDD = null
    val tagRDD = null

    // data pre-processing

    // save data to MongoDB
    storeDataInMongoDB()

    // save data to Elasticsearch
    storeDataInElasticsearch()

    // spark exit
    //    sparkSession.stop()
  }

  def storeDataInMongoDB(): Unit = {
    // TODO
  }

  def storeDataInElasticsearch(): Unit = {
    // TODO
  }
}
