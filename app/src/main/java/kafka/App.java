package kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

public class App 
{
    public static void main( String[] args )
    {
    	SparkSession spark = SparkSession.builder().config("spark.sql.parquet.binaryAsString", "true").appName("kafkaa").getOrCreate();
    	Dataset<Row> df = spark
    			  .readStream()
    			  .format("kafka")
    			  .option("kafka.bootstrap.servers", "hd3713:9000")
    			  .option("subscribe", "rt-adn,rt-adn-gdsp")
    			  .load();
    	df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    	df.printSchema();
    	df.show();
    }
}
