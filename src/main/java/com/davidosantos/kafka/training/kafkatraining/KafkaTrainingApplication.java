package com.davidosantos.kafka.training.kafkatraining;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class KafkaTrainingApplication {

	Logger logger = Logger.getLogger(this.toString());

	Properties props = new Properties();
	KafkaStreams consumer;

	public static void main(String[] args) {

		SpringApplication.run(KafkaTrainingApplication.class, args);
	}

	@PostConstruct
	void doingSetups() {
		logger.info("Doing setups..");
		this.props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092,kafka-2:29092,kafka-3:39092");
		this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-3");
		this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		this.props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		this.props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		// // 1 Exercise
		// //Simple Consumer String
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, String> lines = builder.stream("stream-topic");
		// lines.foreach((key,value) ->{
		// 	logger.info("Hello from simple stream consumer: Key: " + key + " value: " +value);
		// });
		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		//---------------------- 2 -----------------------

		// // 2 Exercise - Using Topology
		// //Using Topology of our own
		// Topology topology = new Topology();
		// topology.addSource("SimpleSource","stream-topic");
		// topology.addProcessor("SimpleProcessor", MyProcessor::new, "SimpleSource");
		// consumer = new KafkaStreams(topology, props);
		// consumer.start();


		// 3 Exercise
		//using streamBuilder e Topology
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> lines = builder.stream("stream-topic");
		lines.print(Printed.<String, String>toSysOut().withLabel("tweets-stream"));
		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();



		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

	}

}
