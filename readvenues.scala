val ctx = com.thinkbiganalytics.spark.LivyWrangler.createSpringContext(sc, sqlContext)
val profiler = ctx.getBean(classOf[com.thinkbiganalytics.spark.dataprofiler.Profiler])
val transformService =
ctx.getBean(classOf[com.thinkbiganalytics.spark.service.TransformService])
val sparkContextService = ctx.getBean(classOf[com.thinkbiganalytics.spark.SparkContextService])
val converterService =
ctx.getBean(classOf[com.thinkbiganalytics.spark.service.DataSetConverterService])
val sparkShellTransformController =
ctx.getBean(classOf[com.thinkbiganalytics.spark.rest.SparkShellTransformController])
val sparkUtilityService =
ctx.getBean(classOf[com.thinkbiganalytics.spark.service.SparkUtilityService])
val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
mapper.registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)

import org.apache.log4j.{Level, Logger}
import com.thinkbiganalytics.spark.logger.LivyLogger
val logger = Logger.getLogger("com.thinkbiganalytics.spark.logger.LivyLogger")

import org.apache.spark.sql._
var df = sqlContext.sql("SELECT tbl10.`venueid`, tbl10.`venuename`, tbl10.`venuecity`, tbl10.`venuestate`, tbl10.`venueseats`, tbl10.`processing_dttm` AS `venues_processing_dttm` FROM `concerts`.`venues` tbl10")
df = df.limit(1000)
df = df.cache(); df.count(); df.registerTempTable( "fc4daee88022443eae238062795ce492" )

LivyLogger.time {
  val (startCol, stopCol) = (0, 1000);
  val lastCol = df.columns.length - 1
  val dfStartCol = if( lastCol >= startCol ) startCol else lastCol
  val dfStopCol = if( lastCol >= stopCol) stopCol else lastCol
  df = df.select( dfStartCol to dfStopCol map df.columns map col: _*)
}

var dfRows : List[Object] = List()
LivyLogger.time {
  val dl = df.collect
  val ( firstRow, lastRow ) = ( 0, dl.size )
  val ( pageStart, pageStop ) = ( 0, 64 )
  val dfStartRow = if( lastRow >= pageStart ) pageStart else lastRow
  val dfStopRow = if( lastRow >= pageStop) pageStop else lastRow

  val pagedRows = dl.slice( dfStartRow, dfStopRow ).map( _.toSeq )
  dfRows = List( df.schema.json, pagedRows )
}
val dfRowsAsJson = mapper.writeValueAsString(dfRows)
