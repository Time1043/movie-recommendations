# project

- 定位

  项目构建

  

- BigPicture

  项目框架、数据源解析

  统计推荐模块、离线推荐模块、实时推荐模块、基于内容的推荐模块



- 数据生命周期

  数据源：关系数据(结构化数据)、日志数据(半结构化)、图片视频(非结构化)

  数据采集：Sqoop、Kafka、Flume、Scribe、ETL

  数据存储：HDFS、HBase、Cassandra、GreenPlum、Oracle

  数据计算：MapReduce、Spark、Flink、Storm、Mahout

  数据应用：可视化 Echarts D3、BI分析、Tableau、业务分析



- 想法

  列表页：实时推荐、离线推荐、统计推荐

  详情页：相似推荐、评分、标签、检索

- 抽象

  实时推荐服务、离线推荐服务、离线统计服务、内容检索服务

  基于模型的推荐、协同过滤的推荐、基于内容的推荐

- 技术选型

  前端框架、后端框架、数据库、大数据

- 架构设计

  前端：用户可视化界面

  后端：Spring、MongoDB(业务数据库)、ElasticSearch(搜索)、Redis(缓存)

  大数据：SparkSQL(离线统计服务)、SparkMLlib(离线推荐服务)；Flume(日志采集)、Kafka(消息缓冲)、SparkStreaming(实时推荐服务)

  



## 数据库设计

- 数据源解析

- 电影信息`movies.csv`

  电影ID `mid`、电影的名称 `name`、电影的描述 `description`、电影的时长 `duration`、电影拍摄时间 `shoot`、电影发布时间 `issue`、

  电影语言 `language`、电影所属类别 `genres`、电影导演 `director`、电影演员 `actors`

  (推荐：**类别**；导演演员...)

- 用户评分信息`ratings.csv`

  用户ID `uid`、电影ID `mid`、电影分值 `score`、评分时间 `timestamp`

- 电影标签信息`tags.csv`

  用户ID `uid`、电影ID `mid`、电影标签 `tag`、评分时间 `timestamp`



- 业务相关table

- 用户表`User`

  用户ID `uid`、用户名 `username`、用户密码 `password`、

  是否第一次登录 `first`、用户偏爱的电影类型 `genres`、用户创建的时间 `timestamp`



- 最近电影评分个数统计表`RateMoreMoviesRecently`

  电影ID `mid`、电影评分数 `count`、评分时段 `yearmonth`

- 电影评分个数统计表`RateMoreMovies`

  电影ID `mid`、电影评分数 `count`

- 电影平均评分表`AverageMoviesScore`

  电影ID `mid`、电影平均评分 `avg`

- 电影相似性矩阵`MovieRecs`

  电影ID `mid`、该电影最相似的电影集合 `recs`



- 用户电影推荐矩阵`UserRecs`  

  用户ID `uid`、推荐给该用户的电影集合 `resc`

- 用户实时电影推荐矩阵`StreamRecs  `

  用户ID `uid`、实时推荐给该用户的电影集合 `resc` 

- 电影类别TOP10`GenresTopMovies  `

  电影类型 `genres`、TOP10 电影 `resc`





## 模块设计

- 模块设计

  ![Snipaste_2024-05-04_20-49-47](res/Snipaste_2024-05-04_20-49-47.png)

- 统计推荐模块

  历史热门电影统计：只考虑评分次数 

  近期热门电影统计：时间条件

  电影平均评分统计：

  各类别TOP10优质电影统计：

  ```sql
  # 历史热门电影统计
  select mid,count(mid) as count from ratings group by mid
  
  # 近期热门电影统计
  
  # 电影平均评分统计
  select mid,avg(score) as avg from ratings group by mid
  
  # 各类别TOP10优质电影统计
  
  
  ```

- 离线推荐模块

  用ALS算法训练隐语义模型：

  计算用户推荐矩阵：

  计算电影相似度矩阵：

- 实时推荐模块

  实时推荐架构：要快、不用准、提前计算、预设推荐模型

  实时推荐优先级计算：跟高分电影相似度很高

- 基于内容的推荐

  电影标签
  
- 分区混合

  基于模型的推荐、协同过滤的推荐、基于内容的推荐、基于统计的推荐





## 项目初始化

- Maven项目

  外层管理：properties版本变量、日志依赖

  内层模块：

  MovieRecommendSystem

  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>
  
      <groupId>org.example</groupId>
      <artifactId>MovieRecommenderSystem</artifactId>
      <packaging>pom</packaging>
      <version>1.0-SNAPSHOT</version>
      <modules>
          <module>recommender</module>
      </modules>
  
      <!-- 声明子项目公用的配置信息 -->
      <properties>
          <log4j.version>1.2.17</log4j.version>
          <slf4j.version>1.7.22</slf4j.version>
          <mongodb-spark.version>2.0.0</mongodb-spark.version>
          <casbah.version>3.1.1</casbah.version>
          <elasticsearch-spark.version>5.6.2</elasticsearch-spark.version>
          <elasticsearch.version>5.6.2</elasticsearch.version>
          <redis.version>2.9.0</redis.version>
          <kafka.version>0.10.2.1</kafka.version>
          <spark.version>2.1.1</spark.version>
          <scala.version>2.11.8</scala.version>
          <jblas.version>1.2.1</jblas.version>
      </properties>
  
      <!-- 声明并引入子项目公用的依赖 -->
      <dependencies>
          <!-- 引入日志管理工具框架 -->
          <dependency>
              <groupId>org.slf4j</groupId>
              <artifactId>jcl-over-slf4j</artifactId>
              <version>${slf4j.version}</version>
          </dependency>
          <dependency>
              <groupId>org.slf4j</groupId>
              <artifactId>slf4j-api</artifactId>
              <version>${slf4j.version}</version>
          </dependency>
          <dependency>
              <groupId>org.slf4j</groupId>
              <artifactId>slf4j-log4j12</artifactId>
              <version>${slf4j.version}</version>
          </dependency>
          <!-- 具体的日志实现 -->
          <dependency>
              <groupId>log4j</groupId>
              <artifactId>log4j</artifactId>
              <version>${log4j.version}</version>
          </dependency>
      </dependencies>
  
      <!-- 仅声明子项目共有的依赖,子项目需要时可引入 -->
      <dependencyManagement>
          <dependencies>
              <dependency>
                  <groupId>org.scala-lang</groupId>
                  <artifactId>scala-library</artifactId>
                  <version>${scala.version}</version>
              </dependency>
          </dependencies>
      </dependencyManagement>
  
      <!-- 声明构建信息 -->
      <build>
          <!-- 声明并引入子项目共有的插件 -->
          <plugins>
              <plugin>
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-compiler-plugin</artifactId>
                  <version>3.6.1</version>
                  <!-- 所有的编译使用JDK1.8 -->
                  <configuration>
                      <source>1.8</source>
                      <target>1.8</target>
                  </configuration>
              </plugin>
          </plugins>
          <!-- 仅声明子项目共有的插件,子项目需要时可引入 -->
          <pluginManagement>
              <plugins>
                  <!-- 该插件用于项目的打包 -->
                  <plugin>
                      <groupId>org.apache.maven.plugins</groupId>
                      <artifactId>maven-assembly-plugin</artifactId>
                      <version>3.0.0</version>
                      <executions>
                          <execution>
                              <id>make-assembly</id>
                              <phase>package</phase>
                              <goals>
                                  <goal>single</goal>
                              </goals>
                          </execution>
                      </executions>
                  </plugin>
                  <!-- 该插件用于将scala代码编译成class文件 -->
                  <plugin>
                      <groupId>net.alchim31.maven</groupId>
                      <artifactId>scala-maven-plugin</artifactId>
                      <version>3.2.2</version>
                      <executions>
                          <!-- 声明绑定到maven的compile阶段 -->
                          <execution>
                              <goals>
                                  <goal>compile</goal>
                                  <goal>testCompile</goal>
                              </goals>
                          </execution>
                      </executions>
                  </plugin>
              </plugins>
          </pluginManagement>
      </build>
  
  </project>
  ```
  
  MovieRecommendSystem\recommender
  
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <parent>
          <artifactId>MovieRecommenderSystem</artifactId>
          <groupId>org.example</groupId>
          <version>1.0-SNAPSHOT</version>
      </parent>
      <modelVersion>4.0.0</modelVersion>
  
      <artifactId>recommender</artifactId>
      <packaging>pom</packaging>
      <modules>
          <module>DataLoader</module>
      </modules>
  
      <!-- 仅声明子项目共有的依赖,子项目需要时可引入 -->
      <dependencyManagement>
          <dependencies>
              <!-- 引入Spark相关的Jar包 -->
              <dependency>
                  <groupId>org.apache.spark</groupId>
                  <artifactId>spark-core_2.11</artifactId>
                  <version>${spark.version}</version>
              </dependency>
              <dependency>
                  <groupId>org.apache.spark</groupId>
                  <artifactId>spark-sql_2.11</artifactId>
                  <version>${spark.version}</version>
              </dependency>
              <dependency>
                  <groupId>org.apache.spark</groupId>
                  <artifactId>spark-streaming_2.11</artifactId>
                  <version>${spark.version}</version>
              </dependency>
              <dependency>
                  <groupId>org.apache.spark</groupId>
                  <artifactId>spark-mllib_2.11</artifactId>
                  <version>${spark.version}</version>
              </dependency>
              <dependency>
                  <groupId>org.apache.spark</groupId>
                  <artifactId>spark-graphx_2.11</artifactId>
                  <version>${spark.version}</version>
              </dependency> <dependency>
              <groupId>org.scala-lang</groupId> <artifactId>scala-library</artifactId> <version>${scala.version}</version>
          </dependency>
          </dependencies>
      </dependencyManagement>
  
      <build>
          <plugins>
              <!-- 父项目已声明该plugin,子项目在引入的时候,不用声明版本和已经声明的配置 -->
              <!-- 该插件用于项目的打包 -->
              <plugin>
                  <groupId>net.alchim31.maven</groupId>
                  <artifactId>scala-maven-plugin</artifactId>
              </plugin>
          </plugins>
      </build>
  
  </project>
  ```
  
  MovieRecommendSystem\recommender\DataLoader
  
  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <parent>
          <artifactId>recommender</artifactId>
          <groupId>org.example</groupId>
          <version>1.0-SNAPSHOT</version>
      </parent>
      <modelVersion>4.0.0</modelVersion>
  
      <artifactId>DataLoader</artifactId>
  
      <dependencies>
          <!-- spark-core的依赖引入 -->
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-core_2.11</artifactId>
          </dependency>
          <!-- spark-sql的依赖引入 -->
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-sql_2.11</artifactId>
          </dependency>
          <!-- scala的依赖引入 -->
          <dependency>
              <groupId>org.scala-lang</groupId>
              <artifactId>scala-library</artifactId>
          </dependency>
          <!-- MongoDB的驱动 -->
          <!-- 用于代码的方式连接MongoDB -->
          <dependency>
              <groupId>org.mongodb</groupId>
              <artifactId>casbah-core_2.11</artifactId>
              <version>${casbah.version}</version>
          </dependency>
          <!-- MongoDB对接spark -->
          <dependency>
              <groupId>org.mongodb.spark</groupId>
              <artifactId>mongo-spark-connector_2.11</artifactId>
              <version>${mongodb-spark.version}</version>
          </dependency>
          <!-- ElasticSearch的驱动 -->
          <!-- 用于代码的方式连接ElasticSearch -->
          <dependency>
              <groupId>org.elasticsearch.client</groupId>
              <artifactId>transport</artifactId>
              <version>${elasticsearch.version}</version>
          </dependency>
          <!-- ElasticSearch对接spark -->
          <dependency>
              <groupId>org.elasticsearch</groupId>
              <artifactId>elasticsearch-spark-20_2.11</artifactId>
              <version>${elasticsearch-spark.version}</version>
              <!-- 用于将不需要依赖的包从依赖路径中除去 -->
              <exclusions>
                  <exclusion>
                      <groupId>org.apache.hive</groupId>
                      <artifactId>hive-service</artifactId>
                  </exclusion>
              </exclusions>
          </dependency>
      </dependencies>
  
  </project>
  
  
  ```
  
  movie-recommendations\MovieRecommendSystem\recommender\DataLoader\src\main\resources\log4j.properties
  
  ```properties
  log4j.rootLogger=info, stdout
  log4j.appender.stdout=org.apache.log4j.ConsoleAppender
  log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
  log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} %5p --- [%50t] %-80c(line:%5L) : %m%n
  ```
  
  



## 相关组件

- ES

  ```bash
  docker network create movie
  
  docker pull elasticsearch:5.6
  docker run -id \
    --name=es \
    -p 9200:9200 \
    -p 9300:9300 \
    --network movie \
    elasticsearch:5.6
  
  docker exec -it es curl -X GET "localhost:9200/_cluster/health?pretty"
  docker exec -it es curl -X GET "localhost:9200/_cat/indices"
  curl http://localhost:9200
  # http://192.168.64.138:9200/
  # http://192.168.64.138:9200/_cat/indices
  
  ```

- MongoDB

  ```bash
  docker pull mongo:3.4.3
  docker run \
    --name mongodb \
    -p 27017:27017 \
    -d \
    --network movie \
    mongo:3.4.3
  
  # centos
  docker exec -it mongodb bash
  mongo
  
  # win
  mongo "mongodb://192.168.64.138:27017"  
  mongo 192.168.64.138:27017
  
  
  show dbs
  use recommender
  show tables
  db.Movie.find().pretty()
  db.Movie.find().count()
  
  ```

  



## recommender.dataloader

- recommender.dataloader

- 定义数据结构 (电影信息 组件连接信息)

  ```scala
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
  ```

- 草稿 (定义常量 + 主要过程)

  ```scala
  object DataLoader {
    // file path
    val MOVIE_DATA_PATH = ""
    val RATING_DATA_PATH = ""
    val TAG_DATA_PATH = ""
  
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
      val sparkConf = null
      val sparkSession = null
  
      // read data to RDD
      val movieRDD = null
      val ratingRDD = null
      val tagRDD = null
        
      // convert RDD to DataFrame
      
  
      // data pre-processing
  
      // write data to MongoDB
      storeDataInMongoDB()
  
      // write data to Elasticsearch
      storeDataInES()
  
      // close spark session
      //    sparkSession.stop()
    }
  
    def storeDataInMongoDB(): Unit = {
      // TODO
    }
  
    def storeDataInES(): Unit = {
      // TODO
    }
  }
  
  ```
  
- more...






## recommender.statistics

- 统计推荐

  历史热门电影统计：只考虑评分次数 

  近期热门电影统计：时间条件

  电影平均评分统计：

  各类别TOP10优质电影统计：



- recommender.statistics

- 定义数据结构 (+ 计算后返回的数据)

  ```scala
  
  case class Movie(mid: Int, name: String, description: String, duration: String, issue: String, shoot: String,
                   language: String, genres: String, actors: String, directors: String)
  
  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
  
  case class MongoConfig(uri: String, db: String)
  
  /**
   * 推荐电影
   * @param mid      电影推荐的id
   * @param score    电影推荐的评分
   */
  case class Recommendation(mid:Int, score:Double)
  
  /**
   * 电影类别推荐
   * @param genres    电影类别
   * @param recs      top10的电影集合
   */
  case class GenresRecommendation(genres:String, recs:Seq[Recommendation])
  
  ```

- 草稿

  ```scala
  object StatisticsRecommender {
    // table name (reading)
    val MONGO_MOVIE_COLLECTION = "Movie"
    val MONGO_RATING_COLLECTION = "Rating"
  
    // table name (writing for statistics)
    val RATE_MORE_MOVIES = "RateMoreMovies"
    val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
    val AVERAGE_MOVIES = "AverageMovies"
    val GENRES_TOP_MOVIES = "GenresTopMovies"
  
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
      // TODO: 历史热门电影统计 (评分次数最多的电影)
      // TODO: 近期热门电影统计 (按照时间排序，年月分组，评分次数最多的电影)
      // TODO: 电影平均评分统计 (所有电影的平均评分)
      // TODO: 电影类别top10推荐 (按照电影类型分组，每个类别top10的电影)
  
      spark.stop()
    }
  }
  
  ```

  



## recommender.Offline

- 基于隐语义模型的协同过滤推荐
- 用户电影推荐矩阵
- 电影相似度矩阵
- 模型评估和参数选取 



- 代码实现

- 定义数据类型

  ```scala
  
  case class Movie(mid: Int, name: String, description: String, duration: String, issue: String, shoot: String,
                   language: String, genres: String, actors: String, directors: String)
  
  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
  
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
  
  ```

- 草稿

  ```scala
  
  ```

  































































































































