package sp.com.org

import org.apache.spark.sql.SparkSession

trait lazyval {

  lazy val spark = SparkSession.builder.master("local[*]").appName("spark_etl").getOrCreate
  lazy val sc =spark.sparkContext

}
