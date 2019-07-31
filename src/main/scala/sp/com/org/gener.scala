package sp.com.org

import org.apache.spark.sql.types.{StringType, StructField, StructType,ArrayType}


object gener extends App with lazyval {
  val schema = StructType(List(
    StructField("budget",StringType, true),
    StructField("genres", StringType, true),
    StructField("homepage",StringType,true),
    StructField("id" ,StringType, true),
    StructField("keywords",StringType, true),
    StructField("original_language",StringType, true),
    StructField("original_title",StringType, true),
    StructField("overview",StringType, true),
    StructField("popularity",StringType, true),
    StructField("production_companies",StringType, true),
    StructField("production_countries",StringType, true),
    StructField("release_date",StringType, true),
    StructField("revenue",StringType, true),
    StructField("runtime",StringType, true),
    StructField("spoken_languages",StringType, true),
    StructField("status",StringType, true),
    StructField("tagline",StringType, true),
    StructField("title",StringType, true),
    StructField("vote_average",StringType, true),
    StructField("vote_count",StringType, true)
  ))

  import org.apache.spark.sql.functions._
  import spark.implicits._
  import org.apache.spark.sql.functions.{lit, schema_of_json}
  val readata = spark.read.option("header","true").option("inferSchema", "true").option("quoteMode", "ALL").option("escape", "\"").schema(schema).csv("/home/indium/Desktop/input_file.csv").toDF()
  readata.createOrReplaceTempView("movie_data")

  //val schemas = schema_of_json(lit(readata.select($"genres").as[String].first))
  //readata.withColumn("genres", from_json($"genres", schema, Map[String, String]())).show()
val readsplit = spark.sql(" select budget,id, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(genres, '[{\"',''),'\"',''),'\"}]\"',''),'}, {',','),'}]','') as gener , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(production_companies, '[{\"',''),'\"',''),'\"}]\"',''),'}, {',','),'}]','') as production_companie FROM movie_data")

readsplit.withColumn("generss",split($"gener",",")(0)).show(false)
  //val rddtest = readata.map()
  //test1.select($"genres").show()

  readsplit.select($"budget",$"gener", explode(split($"gener", ",")).as("gener_spi")).show(false)


  //readata.printSchema()

//  spark.sql("select budget,REGEXP_REPLACE(substr(`genres`,length(`genres`)-60,134),'[(]|[)]','') as genre_id ,length(`genres`) as ids from movie_data").show()
//readata.select(split($"genres"))

  //readata.withColumn("genres", explode(struct($"id"))).show(false)
  //spark.sql("select genereid ,generename from movie_data  lateral view explode(genres) dummy_table as genereid ,generename").show()
  //readata.select(explode($"genres").as("genresa")).select("genres.*").show()
//val maps = StructType(
  //  StructField("longs2strings", createMapType(LongType, StringType), false) :: Nil)
  //val toArray = udf((genres: String) => genres.split(",").map(_.toLong))
  //val test1 = readata.withColumn("genres", toArray(col("genres")))

  //val test1 =readata.withColumn("genres", split(col("genres"), ",").cast("array<>"))
  //test1.printSchema()

  //val result = test1.selectExpr("CAST(genres AS array<struct<id:integer,name:string>>) s2").show()







}
