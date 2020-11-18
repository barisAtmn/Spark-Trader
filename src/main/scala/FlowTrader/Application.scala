package FlowTrader

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.reflect.runtime.universe._

object Application extends App {

  println("Spark is starting...")
  val sparkEngine = SparkSession
    .builder
    .appName("Assignment")
    .master("local[*]")
    .getOrCreate()

  import sparkEngine.implicits._

  case class Trade(id:Long, timestamp:Long, price:Option[Double],quantity:Option[Double])
  case class Price(id:Long, timestamp:Long, bid:Option[Double],ask:Option[Double])
  case class Combined(id:Long, timestamp:Long, bid:Option[Double],ask:Option[Double],price:Option[Double],quantity:Option[Double])
  case class CombinedByTime(id:Long, timestamp:Long, bid:Option[Double],ask:Option[Double],price:Option[Double],quantity:Option[Double],lastTime:Long)

  val windowByPrice = Window.partitionBy('id, 'timestamp).orderBy('lastTime)
  val windowByTrade = Window.partitionBy('id,'timestamp).orderBy('lastTime.desc)



  def classAccessors[T: TypeTag]: List[String] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.toString.split(" ").last
  }.toList.reverse


  """
    Combine the sets of events and fill forward the value columns so that each
    row has the most recent non-null value for the corresponding id. For
    example, given the above input tables the expected output is:

    +---+-------------+-----+-----+-----+--------+
    | id|    timestamp|  bid|  ask|price|quantity|
    +---+-------------+-----+-----+-----+--------+
    | 10|1546300799000| 37.5|37.51| null|    null|
    | 10|1546300800000| 37.5|37.51| 37.5|   100.0|
    | 10|1546300801000| 37.5|37.51|37.51|   100.0|
    | 10|1546300802000|37.51|37.52|37.51|   100.0|
    | 20|1546300804000| null| null|12.67|   300.0|
    | 10|1546300806000| 37.5|37.51|37.51|   100.0|
    | 10|1546300807000| 37.5|37.51| 37.5|   200.0|
    +---+-------------+-----+-----+-----+--------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and filled.
    """
  def fill(tradeDS:Dataset[Trade], priceDS:Dataset[Price]):Dataset[Combined] ={
    import org.apache.spark.sql.functions._

    val trades = tradeDS.join(priceDS.withColumnRenamed("timestamp","price_timestamp").withColumnRenamed("id","price_id"),$"id" <=> $"price_id" && $"price_timestamp".cast(LongType) < $"timestamp".cast(LongType),"left_outer")
      .withColumn("price_last_ts",  when($"price_timestamp".isNotNull,$"price_timestamp".cast(LongType)).otherwise(0))
      .select($"id",$"timestamp",$"bid".as[Option[Double]],$"ask".as[Option[Double]],$"price".as[Option[Double]], $"quantity".as[Option[Double]],$"price_last_ts" as "lastTime")
      .as[CombinedByTime]

    val tradesByRank = trades.withColumn("rank",rank().over(windowByPrice)).filter($"rank" === 1)
      .select($"id",$"timestamp",$"bid".as[Option[Double]],$"ask".as[Option[Double]],$"price".as[Option[Double]], $"quantity".as[Option[Double]])
      .as[Combined]

    val prices = priceDS.join(tradeDS.withColumnRenamed("timestamp","trade_timestamp").withColumnRenamed("id","trade_id"),$"id" <=> $"trade_id" && $"trade_timestamp".cast(LongType) < $"timestamp".cast(LongType),"left_outer")
      .withColumn("trade_last_ts",  when($"trade_timestamp".isNotNull,$"trade_timestamp".cast(LongType)).otherwise(0))
      .withColumn("price_last",  when($"trade_timestamp".cast(LongType) < $"timestamp".cast(LongType),$"price").otherwise(null))
      .withColumn("quantity_last",  when($"trade_timestamp".cast(LongType) < $"timestamp".cast(LongType),$"quantity").otherwise(null))
      .select($"id",$"timestamp",$"bid".as[Option[Double]],$"ask".as[Option[Double]],$"price_last".as[Option[Double]] as "price", $"quantity_last".as[Option[Double]] as "quantity", $"trade_last_ts" as "lastTime")
      .as[CombinedByTime]

    val pricesByRank = prices.withColumn("rank",rank().over(windowByTrade)).filter($"rank" === 1)
      .select($"id",$"timestamp",$"bid".as[Option[Double]],$"ask".as[Option[Double]],$"price".as[Option[Double]], $"quantity".as[Option[Double]])
      .as[Combined]

     pricesByRank.union(tradesByRank).orderBy($"timestamp")
  }


  """
    Pivot and fill the columns on the event id so that each row contains a
    column for each id + column combination where the value is the most recent
    non-null value for that id. For example, given the above input tables the
    expected output is:

    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | id|    timestamp|  bid|  ask|price|quantity|10_bid|10_ask|10_price|10_quantity|20_bid|20_ask|20_price|20_quantity|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+
    | 10|1546300799000| 37.5|37.51| null|    null|  37.5| 37.51|    null|       null|  null|  null|    null|       null|
    | 10|1546300800000| null| null| 37.5|   100.0|  37.5| 37.51|    37.5|      100.0|  null|  null|    null|       null|
    | 10|1546300801000| null| null|37.51|   100.0|  37.5| 37.51|   37.51|      100.0|  null|  null|    null|       null|
    | 10|1546300802000|37.51|37.52| null|    null| 37.51| 37.52|   37.51|      100.0|  null|  null|    null|       null|
    | 20|1546300804000| null| null|12.67|   300.0| 37.51| 37.52|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300806000| 37.5|37.51| null|    null|  37.5| 37.51|   37.51|      100.0|  null|  null|   12.67|      300.0|
    | 10|1546300807000| null| null| 37.5|   200.0|  37.5| 37.51|    37.5|      200.0|  null|  null|   12.67|      300.0|
    +---+-------------+-----+-----+-----+--------+------+------+--------+-----------+------+------+--------+-----------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and pivoted columns.
    """
  def pivot(trades:Dataset[Trade], prices:Dataset[Price]):DataFrame = {
    import org.apache.spark.sql.functions._

    val positions = tradeDS.withColumn("bid",lit(null)).withColumn("ask",lit(null)).select("id","timestamp","bid", "ask","price","quantity").as[Combined]
      .union(priceDS.withColumn("price",lit(null)).withColumn("quantity",lit(null)).select("id","timestamp","bid", "ask","price","quantity").as[Combined])
      .orderBy($"timestamp")

    val pivotArgs = classAccessors[Combined].filter(value => value !="id" && value !="timestamp")

    val idS = positions.select("id").distinct().collect().map(_ (0).toString).toList

    val tempAggregated = idS.foldLeft(positions.toDF())((df, id) => pivotArgs.foldLeft(df)((df, value) => df.withColumn( id + "_" + value ,
      when($"id" === id ,
        col(value)
      )
    )))

    val window = Window.orderBy("timestamp")

    val aggregatedDF = idS.foldLeft(tempAggregated)((df, id) => pivotArgs.foldLeft(df)((df, value) => df.withColumn( id + "_" + value ,
      last(s"${id}_${value}", true).over(window)
    )))

    val columns = ("id" +: pivotArgs) ++ idS.flatMap( id => pivotArgs.map( arg => id + "_" + arg) )

    aggregatedDF.selectExpr(columns : _*)

  }

  val tradeDS = sparkEngine.sparkContext.parallelize(Seq(
    Trade(10L, 1546300800000L, Some(37.50), Some(100.000)),
    Trade(10L, 1546300801000L, Some(37.51), Some(100.000)),
    Trade(20L, 1546300804000L, Some(12.67), Some(300.000)),
    Trade(10L, 1546300807000L, Some(37.50), Some(200.000))
  )).toDS()

  val priceDS = sparkEngine.sparkContext.parallelize(Seq(
    Price(10L, 1546300799000L, Some(37.50), Some(37.51)),
    Price(10L, 1546300802000L, Some(37.51), Some(37.52)),
    Price(10L, 1546300806000L, Some(37.50), Some(37.51)),
  )).toDS()

  fill(tradeDS,priceDS).show(false)
  pivot(tradeDS,priceDS).show(false)



}
