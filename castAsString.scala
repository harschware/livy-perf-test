var dfRows : List[Object] = List()
LivyLogger.time {
  var parent = sqlContext.sql("SELECT * FROM fc4daee88022443eae238062795ce492")
  import org.apache.spark.sql._
  var df = parent
  df = df.select(df("venueid").cast("string"), df("venuename"), df("venuecity"), df("venuestate"), df("venueseats"), df("venues_processing_dttm"))
  df = df.cache(); df.registerTempTable( "d63ed0dddcbf4b4a85155bc92020b2af" )

  val (startCol, stopCol) = (0, 1000);
  val lastCol = df.columns.length - 1
  val dfStartCol = if( lastCol >= startCol ) startCol else lastCol
  val dfStopCol = if( lastCol >= stopCol) stopCol else lastCol
  df = df.select( dfStartCol to dfStopCol map df.columns map col: _*)

  val dl = df.collect
  val ( firstRow, lastRow ) = ( 0, dl.size )
  val ( pageStart, pageStop ) = ( 0, 64 )
  val dfStartRow = if( lastRow >= pageStart ) pageStart else lastRow
  val dfStopRow = if( lastRow >= pageStop) pageStop else lastRow

  val pagedRows = dl.slice( dfStartRow, dfStopRow ).map( _.toSeq )
  dfRows = List( df.schema.json, pagedRows )
}
var dfRowsAsJson = mapper.writeValueAsString(dfRows)
%json dfRowsAsJson
