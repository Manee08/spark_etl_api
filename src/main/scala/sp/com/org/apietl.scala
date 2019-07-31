package sp.com.org


object apietl extends App with lazyval {

  val readdatasql =spark.read.format("jdbc").option("url","jdbc:postgresql://localhost/postgres?setAutoCommit=true")
  .option("dbtable", "public.test")
  .option("user", "postgres")
  .option("password", "postgres")
  .option("driver", "org.postgresql.Driver")
  .load()

  val transform = readdatasql.select("id","username").where("id <= 5")
  transform.write.format("org.apache.spark.sql.cassandra").mode("overwrite")
    .option("spark.cassandra.connection.host","127.0.0.1")
    .option("spark.cassandra.connection.port","9042")
    .option("confirm.truncate","true").option("keyspace","sparkconnect")
    .option("table","res")
    .save()




}
