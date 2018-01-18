package com.demo.flink.streaming;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Properties;

public class StreamingJob {

	public static void main(String[] args) throws Exception {

		final String selfDestructCode = args[0];

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "kafka:9092");
		properties.setProperty("group.id", "flink");

		FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>("mytopic", new SimpleStringSchema(),
				properties);

		DataStreamSource<String> source = env.addSource(myConsumer);

		SingleOutputStreamOperator<String> mapped = source.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				if (selfDestructCode.equals(value)) {
					throw new RuntimeException("Boom!");
				}
				return value;
			}
		});

		mapped.print();

		env.execute("Flink application");
	}

}
