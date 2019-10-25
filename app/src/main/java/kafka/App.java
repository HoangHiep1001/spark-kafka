package kafka;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Read {
	public static final String HOST_KAFKA = "10.5.36.64:9092,10.5.36.68:9092,"
			+ "10.5.36.71:9092,10.5.36.78:9092,10.5.36.80:9092,10.5.36.81:9092,10.5.36.83:9092,"
			+ "10.5.36.84:9092,10.5.36.87:9092,10.5.36.91:9092";

	public static void main(String[] args) throws InterruptedException {
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", HOST_KAFKA);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkKafka");
		Collection<String> topics = Arrays.asList("rt-adn-gdsp", "rt-adn");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(30));
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		//stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		JavaPairDStream<String, String> mappedStream = 
				stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		mappedStream.print();
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
