import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

/** A simple example job that cleans input data for some equity option trades
 *  and store them in files to be used in another job, SampleCreator.
 *
 *  Files are generated for some Eurex listed options on equities quoted at
 *  the Helsinki stock exchange on a historical date. These data files are 
 *  subject to copyright and shall not be distributed elsewhere. Please see
 *  the LICENSE file for detales.
 *
 *  You can generate your own files by downloading more recent input data but 
 *  please bear in mind that this program requires specific file formats and
 *  does not allow for missing data. You will also have to modify NasDaq OMX
 *  Nordic's equity prices files, changing the decimal separators from ","
 *  (comma) to "." (point).
 */

  // Initializing the log to register execution times
  val simpleLog = new SimpleLog("DataCleaner")
  simpleLog.beginProgram
  
  // Dates and horizons, should be equal for all applications in this package
  val today = "2019-09-27"
  val yesterday =  "2019-09-26"
  val historical = "2019-03-26"
  val volObs = 100 // Number of observations used for estimating volatility
  
  // Filters
  val equities = Array("ERIBR", "NOKIA", "SAMPO", "STE-R", "TELIA1", "TIETO")
  
  // Input file names
  val inputPath = "/margincalls/data/"
  val codesFile = s"${inputPath}vendorcodes.csv"
  val seriesFile = s"${inputPath}ct${today.filter(!"-".contains(_))}.txt"
  val historicalFiles = equities.map(inputPath + _ + s"-${historical}-${today}.csv")
  
  // Output file names (e.g. directories where Spark will write partial files)
  val outputPath = "/margincalls/data/"
  // val equitiesDir = s"${outputPath}equities"
  val instrumentsDir = s"${outputPath}instruments"
  val pricesDirs = equities.map(outputPath + _ + "-prices")

  // Creating equities and series correspondence
  simpleLog.writeMsg("Creating equities and series correspondences")
  
  val equitiesDF = spark.createDataFrame(Seq(
    ("ERCB", "ERIBR"),
    ("ENUR", "STE-R"),
    ("NOA1", "NOKIA"),
    ("NOA2", "NOKIA"),
    ("NOA3", "NOKIA"),
    ("NOA4", "NOKIA"),
    ("NOA5", "NOKIA"),
    ("NOAE", "NOKIA"),
    ("SMPA", "SAMPO"),
    ("TLSN", "TELIA1"),
    ("TTEB", "TIETO")
  )).toDF("PRODUCT", "FILE_PREFIX")

  equitiesDF.cache()
  
  // Loading vendor codes from file
  simpleLog.writeMsg("Loading vendor codes from file")
  val codesDF = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("mode", "DROPMALFORMED")
    .load(codesFile)
    .filter("`LEGAL PRODUCT GROUP` = 'SINGLE STOCK OPTIONS'")
	
  codesDF.collect()
  
  // Loading currently traded series from file
  simpleLog.writeMsg("Loading currently traded series from file")
  
  val seriesSchema = StructType(Array(
    StructField("FIRST_CHAR", StringType, false),
    StructField("PRODUCT", StringType, false),
    StructField("CALL_PUT_OR_PUTURE", StringType, false),
    StructField("EXPIRY_OR_LAST_TRADING", DateType, false),
    StructField("EMPTY", StringType, true),
    StructField("CURRENT_STRIKE", DoubleType, false),
    StructField("CURRENT_STRIKE_DISPLAY_DECIMALS", IntegerType, false),
    StructField("VERSION_NUMBER", IntegerType, false),
    StructField("STATUS", StringType, false),
    StructField("ISIN", StringType, false),
    StructField("EXCHANGE", StringType, false),
    StructField("CURRENCY", StringType, false),
    StructField("CURRENCY_DECIMAL_SHIFT", IntegerType, false),
    StructField("TYPE_MONTH_CODE", StringType, false),
    StructField("EXPIRATION_YEAR", IntegerType, false),
    StructField("ORIGINAL_STRIKE", DoubleType, false),
    StructField("GENERATION_NUMBER", IntegerType, false),
    StructField("ORIGINAL_STRIKE_DECIMAL_PLACES", IntegerType, false),
    StructField("SHARES_PER_CONTRACT", DoubleType, false),
    StructField("DELIVERY_MULTIPLIER", DoubleType, false)
  ))

  val seriesDF = spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", " ")
    .option("mode", "DROPMALFORMED")
    .option("comment", "!")
    .option("dateformat", "yyyyMMdd")
    .schema(seriesSchema)
    .load(seriesFile)
    .filter("FIRST_CHAR = '|'")
	.filter("CURRENT_STRIKE != 0")
    .filter("CALL_PUT_OR_PUTURE != 'F'")
    .filter("STATUS = 'A'")
    .filter("EXCHANGE = 'XHEX'")
    .filter("CURRENCY = 'EUR'")
	
  seriesDF.collect()
   
  // Loading historical equity prices from files
  simpleLog.writeMsg("Loading historical equity prices from files")
  
  val pricesDFBuffer = new ArrayBuffer[DataFrame]
	
  for (i <- 0 to equities.length - 1) {
    pricesDFBuffer.append(spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("comment", "s")
      .option("mode", "DROPMALFORMED")
      .option("dateformat", "yyyyMMdd")
      .load(historicalFiles(i))
	  .selectExpr("Date AS DATE", "`Closing price` AS PRICE")
	  .withColumn("EQUITY", lit(equities(i))))
	  
	pricesDFBuffer(i).collect()
  }
  
  // Writing instruments to CSV file
  simpleLog.writeMsg("Writing instruments to CSV file")
  
  val equitiesJoin = seriesDF.col("PRODUCT") === equitiesDF.col("PRODUCT")
  val codesJoin = seriesDF.col("PRODUCT") === codesDF.col("PRODUCT ID")

  /** Returns a dataframe where an index column has been added.
   *
   *  @param df the dataframe to which the index should be added
   */
  def addColumnIndex(df: DataFrame) = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      StructType(df.schema.fields :+ StructField("INSTRUMENT_ID", LongType, false)))
  }
  
  val instrumentsDF = addColumnIndex(seriesDF.join(equitiesDF, equitiesJoin)
    .join(codesDF, codesJoin)
	.drop(equitiesDF.col("PRODUCT"))
	.drop(codesDF.col("PRODUCT ID"))
	.filter("CURRENT_STRIKE > 1"))
	.selectExpr(
	  "INSTRUMENT_ID",
      "PRODUCT",
	  "FILE_PREFIX AS UNDERLYING",
      "CALL_PUT_OR_PUTURE AS CALL_PUT",
      "EXPIRY_OR_LAST_TRADING AS EXPIRY",
      "CURRENT_STRIKE / pow(10, CURRENT_STRIKE_DISPLAY_DECIMALS) AS STRIKE",
      "EXCHANGE",
      "CURRENCY",
      "SHARES_PER_CONTRACT AS LOT_SIZE")
	
  instrumentsDF.coalesce(1).write.format("csv").mode("overwrite")
    .option("delimiter", ";").option("header", "true")
	.save(s"${instrumentsDir}.csv")

 
  // Writing historical prices to CSV files
  simpleLog.writeMsg("Writing historical prices to CSV files")

  for (i <- 0 to equities.length - 1) {
    pricesDFBuffer(i).coalesce(1).write.format("csv").mode("overwrite")
    .option("delimiter", ";").option("header", "true")
	.save(s"${pricesDirs(i)}.csv")
  }
  
  // End of program
  simpleLog.endProgram

  Thread.sleep(1000) // Ensures that log is written to console
