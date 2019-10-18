import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{exp, log, pow, sqrt}

import org.apache.commons.math3.random.MersenneTwister
import org.apache.commons.math3.special.Erf.erf

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** A simple example app that calculates margin calls for a random sample of
 *  deals between a set of counterparts.
 *
 *  The input files should have been generated previously by the application
 *  DealSampler, also provided in this package. 
 */
 
  // Initializing the log to register execution times
  val simpleLog = new SimpleLog("MarginCaller")
  simpleLog.beginProgram

  // Dates and horizons, should be equal for all applications in this package
  val today = "2019-09-27"
  val yesterday =  "2019-09-26"
  val historical = "2019-03-26"
  val volObs = 100 // Number of observations used for estimating volatility
  
  // Sample size
  val numberOfTrades = 1000000
  val numberOfCptys = 1000
  
  /** We choose to simplify by using a fixed risk-free interest rate instead of
   *  applying interpolations based on an interest rate curve.
   */
  val interestRate = 0.01 // 1%
  
  // Filters
  val equities = Array("ERIBR", "NOKIA", "SAMPO", "STE-R", "TELIA1", "TIETO")
  
  // Input file names (e.g. directories where Spark will read partial files)
  val inputPath = "/margincalls/data/"
  val dealsDir = s"${inputPath}deals.csv"
  val instrumentsDir = s"${inputPath}instruments.csv"
  val pricesDirs = equities.map(inputPath + _ + "-prices.csv")
  
  // Output file names (e.g. directories where Spark will write partial files)
  val outputPath = "/margincalls/data/"
  val margincallsDir = s"${outputPath}margincalls"
  
  // Loading input files
  simpleLog.writeMsg("Loading input files")
  
  val dealsSchema = StructType(Array(
    StructField("BUYER_ID", StringType, false),
    StructField("SELLER_ID", StringType, false),
    StructField("VOLUME", IntegerType, false),
    StructField("INSTRUMENT_ID", IntegerType, false),
    StructField("PRODUCT", StringType, false),
    StructField("UNDERLYING", StringType, false),
    StructField("CALL_PUT", StringType, false),
    StructField("EXPIRY", StringType, false),
    StructField("STRIKE", DoubleType, false),
    StructField("EXCHANGE", StringType, false),
    StructField("CURRENCY", StringType, false),
    StructField("LOT_SIZE", DoubleType, false)
  ))
  
  val dealsDF = spark.read.schema(dealsSchema)
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("mode", "DROPMALFORMED")
      .option("dateformat", "yyyy-MM-dd")
      .load(dealsDir)
	  
  dealsDF.cache()

  val instrumentsSchema = StructType(Array(
    StructField("INSTRUMENT_ID", IntegerType, false),
    StructField("PRODUCT", StringType, false),
    StructField("UNDERLYING", StringType, false),
    StructField("CALL_PUT", StringType, false),
    StructField("EXPIRY", StringType, false),
    StructField("STRIKE", DoubleType, false),
    StructField("EXCHANGE", StringType, false),
    StructField("CURRENCY", StringType, false),
    StructField("LOT_SIZE", DoubleType, false)))
	  
  val instrumentsDF = spark.read.schema(instrumentsSchema)
    .format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("mode", "DROPMALFORMED")
    .option("dateformat", "yyyy-MM-dd")
    .load(instrumentsDir)
	
  instrumentsDF.cache()

  val pricesDFBuffer = new ListBuffer[DataFrame]
  
  val pricesSchema = StructType(Array(
    StructField("DATE", StringType, false),
    StructField("PRICE", DoubleType, false),
    StructField("EQUITY", StringType, false)))
  
  for (i <- 0 to equities.length - 1)
    pricesDFBuffer.append(spark.read.schema(pricesSchema)
      .format("csv")
      .option("header", "true")
      .option("delimiter", ";")
      .option("mode", "DROPMALFORMED")
      .option("dateformat", "yyyy-MM-dd")
      .load(pricesDirs(i))
	  .limit(volObs))
	  
  // Preparing volatilities and underlying prices
  simpleLog.writeMsg("Preparing volatilities and underlying prices")
  
  val marketDataDFBuffer = new ListBuffer[DataFrame]
  
  for (i <- 0 to equities.length - 1) {
    val volatilityDF = pricesDFBuffer(i).groupBy("EQUITY")
	  .agg(stddev_pop("PRICE")).toDF("EQUITY", "VOLATILITY")
 
    val todaysPriceDF = pricesDFBuffer(i).selectExpr("EQUITY", "PRICE AS PRICE_TODAY")
	 .where(col("DATE") === (lit(today)))
	 
    val yesterdaysPriceDF = pricesDFBuffer(i).selectExpr("EQUITY", "PRICE AS PRICE_YESTERDAY")
	 .where(col("DATE") === (lit(yesterday)))

    val todaysPriceJoin = 
      volatilityDF.col("EQUITY") === todaysPriceDF.col("EQUITY")
    val yesterdaysPriceJoin = 
      volatilityDF.col("EQUITY") === yesterdaysPriceDF.col("EQUITY")

    val tempDF = volatilityDF.join(todaysPriceDF, todaysPriceJoin)
	  .drop(todaysPriceDF.col("EQUITY"))
	  .join(yesterdaysPriceDF, yesterdaysPriceJoin)
	  .drop(yesterdaysPriceDF.col("EQUITY"))
	  
    marketDataDFBuffer.append(tempDF)
  }
  
  val marketDataDF = marketDataDFBuffer.reduce(_ union _)
  
  marketDataDF.cache()
    
  // Preparing instrument valuations
  simpleLog.writeMsg("Preparing instrument valuations")
  
  val marketDataJoin = 
    instrumentsDF.col("UNDERLYING") === marketDataDF.col("EQUITY")
	

	
  /** Returns the value of a European equity option obtained with the well-known
   *  Black-Scholes formula.
   *
   *  A user defined functions (UDF) that takes columns as arguments
   *
   *  @param S the price of the underlying equity at time t
   *  @param K the option's strike
   *  @param r the annualized, continuously compounded risk-free interest rate 
   *  @param v the volatility of the underlying equity
   *  @param t the time to expiry in years from now
   *  @param CP call or put option
   */   	
  def blackscholes_udf = udf( (S: Double, K: Double, r: Double, v: Double, 
      t: Double, CP: String) => {
	    val d1 = (log(S / K) + (r + pow(v, 2) / 2) * t) / (v * sqrt(t))
	    val d2 = (log(S / K) + (r - pow(v, 2) / 2) * t) / (v * sqrt(t))

       /** Returns the value of the cumulative distribution function of a standard
        *  normal distribution for a given input value
        *  @param x the given input value
        */
        def cdf(x: Double) = 0.5 * (1 + erf(x / sqrt(2)))
	
	    if (CP == "P") cdf(-d2) * K * exp(-r * t) - S * cdf(-d1)
	    else cdf(d1) * S - cdf(d2) * K * exp(-r * t)
	  })
  
  // Registering the UDF makes it available in all worker machines
  spark.udf.register("blackscholes_udf", blackscholes_udf)
    
  val instrMktDataDF = instrumentsDF.join(marketDataDF, marketDataJoin)
    .drop(marketDataDF.col("EQUITY"))
	.withColumn("TODAY_TO_EXPIRY", datediff(col("EXPIRY"), lit(today)) / 365.0)
	.withColumn("YESTERDAY_TO_EXPIRY", datediff(col("EXPIRY"), lit(yesterday))
      / 365.0)
	.withColumn("RISK_FREE_RATE", lit(interestRate))

  val instrumentsValuationsDF = instrMktDataDF
    .withColumn("VALUE_TODAY", blackscholes_udf(instrMktDataDF("PRICE_TODAY"),
	  instrMktDataDF("STRIKE"), instrMktDataDF("RISK_FREE_RATE"),
	  instrMktDataDF("VOLATILITY"), instrMktDataDF("TODAY_TO_EXPIRY"),
	  instrMktDataDF("CALL_PUT")))
    .withColumn("VALUE_YESTERDAY", blackscholes_udf(instrMktDataDF("PRICE_TODAY"),
	  instrMktDataDF("STRIKE"), instrMktDataDF("RISK_FREE_RATE"),
	  instrMktDataDF("VOLATILITY"), instrMktDataDF("YESTERDAY_TO_EXPIRY"),
	  instrMktDataDF("CALL_PUT")))
	.selectExpr(
	  "INSTRUMENT_ID",
	  "PRODUCT",
	  "CALL_PUT",
	  "STRIKE",
	  "EXPIRY",
	  "VALUE_TODAY - VALUE_YESTERDAY AS VALUE_CHANGE"
	)
	  
  instrumentsValuationsDF.cache()
  
  // Preparing deal valuations
  simpleLog.writeMsg("Preparing deal valuations")
  
  val instrumentsValuationsJoin =
    dealsDF("INSTRUMENT_ID") === instrumentsValuationsDF("INSTRUMENT_ID")
	
  def position_change_udf = udf( (volume: Double, lotSize: Double,
     valueChange: Double) => volume * lotSize * valueChange )
  
  spark.udf.register("position_change_udf", position_change_udf) 
  
  val dealsValuationsDF =
    dealsDF.join(instrumentsValuationsDF, instrumentsValuationsJoin)
	.withColumn("POSITION_CHANGE", position_change_udf(dealsDF("VOLUME"),
	  dealsDF("LOT_SIZE"), instrumentsValuationsDF("VALUE_CHANGE")))

  dealsValuationsDF.cache()
  
  // Preparing margin calls for buyers
  simpleLog.writeMsg("Preparing margin calls for buyers")
  
  def addFunc = (a: Double, b: Double) => a + b
  
  val buyerMarginCallsDF = spark.createDataFrame(dealsValuationsDF
    .selectExpr("BUYER_ID", "POSITION_CHANGE").rdd.map {
      case Row(key: String, value: Double) => key -> value
    }.reduceByKey(addFunc)).toDF("CPTY_ID", "MARGIN_CALL")

  // Preparing margin calls for sellers  
  simpleLog.writeMsg("Preparing margin calls for sellers")
  
  val sellerMarginCallsDF = spark.createDataFrame(dealsValuationsDF
    .selectExpr("SELLER_ID", "-1.0 * POSITION_CHANGE").rdd.map {
      case Row(key: String, value: Double) => key -> value
    }.reduceByKey(addFunc)).toDF("CPTY_ID", "MARGIN_CALL")
 
  // Preparing total margin call per counterpart
  simpleLog.writeMsg("Preparing total margin call per counterpart")
  
  val marginCallsDF = spark.createDataFrame(buyerMarginCallsDF
    .union(sellerMarginCallsDF).rdd.map {
      case Row(key: String, value: Double) => key -> value
    }.reduceByKey(addFunc)).toDF("CPTY_ID", "MARGIN_CALL") 
	
  // Writing margin calls to CSV file
  simpleLog.writeMsg("Writing margin calls to CSV file")
  
  def format_cpty_udf = udf( (cpty_id: String) => 
    s"CPTY ${cpty_id.reverse.padTo(4, '0').reverse}" )
  
  spark.udf.register("format_cpty_udf", format_cpty_udf) 
  
  def rounding_udf = udf( (amount: Double) => 
    (amount * 100.0).round / 100.0 )
  
  spark.udf.register("rounding_udf", rounding_udf) 
  
  
  marginCallsDF.coalesce(1)
    .withColumn("COUNTERPART", format_cpty_udf(marginCallsDF("CPTY_ID")))
	.withColumn("MARGIN CALL", rounding_udf(marginCallsDF("MARGIN_CALL")))
	.select("COUNTERPART", "MARGIN CALL")
    .orderBy("COUNTERPART").write.format("csv").mode("overwrite")
	.option("delimiter", ";").option("header", "true")
	.save(s"${margincallsDir}.csv")

  // End of program
  simpleLog.endProgram

  Thread.sleep(1000) // Ensures that log is written to console
