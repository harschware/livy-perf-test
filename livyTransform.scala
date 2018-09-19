var tbl :String = null;
var parent = try {
  tbl = """b75b6abb76684dc1b08eef66a8b68276""";
  var df = sqlContext.read.table(tbl)
  logger.info("Parent dataframe set to table '{}'", tbl);
  df
} catch { case _ : Exception =>  {
  logger.info("Unable to load data from parent table '{}'.  Rebuild from transformation history.", tbl);
import org.apache.spark.sql._
var df = sqlContext.sql("SELECT tbl10.`venueid`, tbl10.`venuename`, tbl10.`venuecity`, tbl10.`venuestate`, tbl10.`venueseats`, tbl10.`processing_dttm` AS `venues_processing_dttm` FROM `concerts`.`venues` tbl10")
df = df.limit(1000)
df

}}
import org.apache.spark.sql._
var df = parent
df = df.select(df("venueid"), df("venuename"), df("venuecity"), functions.lower(df("venuestate")).as("venuestate"), df("venueseats"), df("venues_processing_dttm"))
df.registerTempTable( "c4bbbbfd0aad40c8a9cf05a7d315fd09" )
