//import org.apache.spark.sql._
var df = sqlContext.read.table("b75b6abb76684dc1b08eef66a8b68276")
df = df.select(df("venueid"), df("venuename"), df("venuecity"), functions.lower(df("venuestate")).as("venuestate"), df("venueseats"))
df.registerTempTable( "c4bbbbfd0aad40c8a9cf05a7d315fd09" )
