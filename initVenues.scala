import org.apache.spark.sql._
var df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("file:///tmp/venue.csv")
df = df.limit(1000)
df.registerTempTable( "b75b6abb76684dc1b08eef66a8b68276" )
df
