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

  movie-recommendations\MovieRecommendSystem\pom.xml

  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>
  
      <groupId>com.time1043</groupId>
      <artifactId>MovieRecommendSystem</artifactId>
      <version>1.0-SNAPSHOT</version>
      <packaging>pom</packaging>
      <modules>
          <module>recommender</module>
          <module>recommender/DataLoader</module>
      </modules>
  
      <properties>
          <maven.compiler.source>8</maven.compiler.source>
          <maven.compiler.target>8</maven.compiler.target>
          <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  
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
  
      <dependencies>
          <!-- 引入共同的日志管理工具 -->
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
          <dependency>
              <groupId>log4j</groupId>
              <artifactId>log4j</artifactId>
              <version>${log4j.version}</version>
          </dependency>
      </dependencies>
  
      <build>
          <!--声明并引入子项目共有的插件-->
          <plugins>
              <plugin>
                  <groupId>org.apache.maven.plugins</groupId>
                  <artifactId>maven-compiler-plugin</artifactId>
                  <version>3.6.1</version>
                  <!--所有的编译用 JDK1.8-->
                  <configuration>
                      <source>1.8</source>
                      <target>1.8</target>
                  </configuration>
              </plugin>
          </plugins>
          <pluginManagement>
              <plugins>
                  <!--maven 的打包插件-->
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
                  <!--该插件用于将 scala 代码编译成 class 文件-->
                  <plugin>
                      <groupId>net.alchim31.maven</groupId>
                      <artifactId>scala-maven-plugin</artifactId>
                      <version>3.2.2</version>
                      <executions>
                          <!--绑定到 maven 的编译阶段-->
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

  movie-recommendations\MovieRecommendSystem\recommender\pom.xml

  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>
      <parent>
          <groupId>com.time1043</groupId>
          <artifactId>MovieRecommendSystem</artifactId>
          <version>1.0-SNAPSHOT</version>
      </parent>
  
      <artifactId>recommender</artifactId>
  
      <properties>
          <maven.compiler.source>8</maven.compiler.source>
          <maven.compiler.target>8</maven.compiler.target>
          <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
      </properties>
  
      <dependencyManagement>
          <dependencies>
              <!-- 引入 Spark 相关的 Jar 包 -->
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
              </dependency>
              <dependency>
                  <groupId>org.scala-lang</groupId>
                  <artifactId>scala-library</artifactId>
                  <version>${scala.version}</version>
              </dependency>
          </dependencies>
      </dependencyManagement>
  
      <build>
          <plugins>
              <!-- 父项目已声明该 plugin，子项目在引入的时候，不用声明版本和已经声明的配置 -->
              <plugin>
                  <groupId>net.alchim31.maven</groupId>
                  <artifactId>scala-maven-plugin</artifactId>
              </plugin>
          </plugins>
      </build>
  
  </project>
  ```

  movie-recommendations\MovieRecommendSystem\recommender\DataLoader\pom.xml

  ```xml
  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
      <modelVersion>4.0.0</modelVersion>
      <parent>
          <groupId>com.time1043</groupId>
          <artifactId>MovieRecommendSystem</artifactId>
          <version>1.0-SNAPSHOT</version>
          <relativePath>../../pom.xml</relativePath>
      </parent>
  
      <artifactId>DataLoader</artifactId>
  
      <properties>
          <maven.compiler.source>8</maven.compiler.source>
          <maven.compiler.target>8</maven.compiler.target>
          <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
      </properties>
  
      <dependencies>
          <!-- Spark 的依赖引入 -->
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-core_2.11</artifactId>
          </dependency>
          <dependency>
              <groupId>org.apache.spark</groupId>
              <artifactId>spark-sql_2.11</artifactId>
          </dependency>
          <!-- 引入 Scala -->
          <dependency>
              <groupId>org.scala-lang</groupId>
              <artifactId>scala-library</artifactId>
          </dependency>
          <!-- 加入 MongoDB 的驱动 -->
          <dependency>
              <groupId>org.mongodb</groupId>
              <artifactId>casbah-core_2.11</artifactId>
              <version>${casbah.version}</version>
          </dependency>
          <dependency>
              <groupId>org.mongodb.spark</groupId>
              <artifactId>mongo-spark-connector_2.11</artifactId>
              <version>${mongodb-spark.version}</version>
          </dependency>
          <!-- 加入 ElasticSearch 的驱动 -->
          <dependency>
              <groupId>org.elasticsearch.client</groupId>
              <artifactId>transport</artifactId>
              <version>${elasticsearch.version}</version>
          </dependency>
          <dependency>
              <groupId>org.elasticsearch</groupId>
              <artifactId>elasticsearch-spark-20_2.11</artifactId>
              <version>${elasticsearch-spark.version}</version>
              <!-- 将不需要依赖的包从依赖路径中除去 -->
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

  









































































































































