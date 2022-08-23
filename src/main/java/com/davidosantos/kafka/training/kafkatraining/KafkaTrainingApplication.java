package com.davidosantos.kafka.training.kafkatraining;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

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
		this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-4");
		this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		this.props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		this.props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		this.props.put("schema.registry.url", "http://schema-registry:8081");

		// ------------------------ Statefuless Operations -------------------------
		// ---------------------- 1 -----------------------

		// // 1 Exercise
		// //Simple Consumer String
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, String> lines = builder.stream("stream-topic");
		// lines.foreach((key,value) ->{
		// logger.info("Hello from simple stream consumer: Key: " + key + " value: "
		// +value);
		// });
		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 2 -----------------------

		// // 2 Exercise - Using Topology
		// //Using Topology of our own
		// Topology topology = new Topology();
		// topology.addSource("SimpleSource","stream-topic");
		// topology.addProcessor("SimpleProcessor", MyProcessor::new, "SimpleSource");
		// consumer = new KafkaStreams(topology, props);
		// consumer.start();

		// ---------------------- 3 -----------------------

		// // 3 Exercise
		// //using streamBuilder e Topology
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, String> lines = builder.stream("stream-topic");
		// lines.print(Printed.<String, String>toSysOut().withLabel("tweets-stream"));
		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 4 -----------------------

		// 4 Exercise
		// //using streamBuilder and Topology with custom Serdes
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));
		// lines.print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("SimplePojoObject-stream"));
		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 5 -----------------------

		// // 5 Exercise
		// //using streamBuilder and Topology with custom Serdes, and filtering
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));
		// lines.filter((key,simplePojoObject) -> simplePojoObject.getAge()>=34)
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("SimplePojoObject-stream"));
		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 6 -----------------------

		// // 6 Exercise
		// //using streamBuilder and Topology with custom Serdes, and filtering Not
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));
		// lines.filterNot((key,simplePojoObject) -> simplePojoObject.getAge()>=34)
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("SimplePojoObject-stream"));
		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 7 -----------------------

		// // 7 Exercise
		// // using streamBuilder and Topology with custom Serdes, and branching results
		// //Used to create separate KStream's with split
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));

		// Map<String, KStream<String, SimplePojoObject>> branchs =
		// lines.split(Named.as("branch-")) //if not set a name fakfka will put default
		// name
		// .branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
		// .branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
		// .noDefaultBranch();

		// if (branchs.containsKey("branch-greaterThan34")) //concatened as defined
		// above
		// branchs.get("branch-greaterThan34")
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		// if (branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-lessThan34")
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 8 -----------------------

		// // 8 Exercise
		// // using streamBuilder and Topology with custom Serdes, and branching results
		// // Used to create separate KStream's with split, using Map to rekey
		// //there is also a mapValues, used to map any field of the data
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));

		// Map<String, KStream<String, SimplePojoObject>> branchs =
		// lines.split(Named.as("branch-")) // if not set a name
		// // fakfka will put
		// // default name
		// .branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
		// .branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
		// .noDefaultBranch();

		// if (branchs.containsKey("branch-greaterThan34")) // concatened as defined
		// above
		// branchs.get("branch-greaterThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		// if (branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-lessThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 9 -----------------------

		// // 9 Exercise
		// // using streamBuilder and Topology with custom Serdes, and branching results
		// // Used to create separate KStream's with split, using Map to rekey
		// // using Merce
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));

		// Map<String, KStream<String, SimplePojoObject>> branchs =
		// lines.split(Named.as("branch-")) // if not set a name
		// // fakfka will put
		// // default name
		// .branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
		// .branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
		// .noDefaultBranch();

		// if (branchs.containsKey("branch-greaterThan34")) // concatened as defined
		// above
		// branchs.get("branch-greaterThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		// if (branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-lessThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		// if (branchs.containsKey("branch-greaterThan34") &&
		// branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("Merged"));

		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 10 -----------------------

		// // 10 Exercise
		// // using streamBuilder and Topology with custom Serdes, and branching results
		// // Used to create separate KStream's with split
		// // using flatMap can be used to extracts fields of the data class or make any
		// // operation with the data
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));

		// Map<String, KStream<String, SimplePojoObject>> branchs =
		// lines.split(Named.as("branch-")) // if not set a name
		// // fakfka will put
		// // default name
		// .branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
		// .branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
		// .noDefaultBranch();

		// if (branchs.containsKey("branch-greaterThan34")) // concatened as defined
		// above
		// branchs.get("branch-greaterThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		// if (branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-lessThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		// List<Integer> ages = new ArrayList<>();

		// if (branchs.containsKey("branch-greaterThan34") &&
		// branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
		// .flatMapValues((simplePojo) -> {
		// ages.add(simplePojo.getAge());
		// logger.info("ages are:" + ages);
		// return ages;
		// }).foreach((key, v) -> {
		// logger.info("key: " + key);
		// logger.info("v: " + v);
		// });
		// ;

		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// ---------------------- 11 -----------------------

		// // // 11 Exercise
		// // // using streamBuilder and Topology with custom Serdes, and branching
		// results
		// // // Used to create separate KStream's with split
		// // // producing data to another topic using avro.
		// // // dependendy necessary to import avro Serdes ->
		// // // io.confluent:kafka-streams-avro-serde
		// // // <!--
		// // https://mvnrepository.com/artifact/io.confluent/kafka-streams-avro-serde
		// // // -->
		// // // <dependency>
		// // // <groupId>io.confluent</groupId>
		// // // <artifactId>kafka-streams-avro-serde</artifactId>
		// // // <version>7.2.1</version>
		// // // </dependency>
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));

		// Map<String, KStream<String, SimplePojoObject>> branchs =
		// lines.split(Named.as("branch-")) // if not set a name
		// // fakfka will put
		// // default name
		// .branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
		// .branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
		// .noDefaultBranch();

		// if (branchs.containsKey("branch-greaterThan34")) // concatened as defineda
		// bove
		// branchs.get("branch-greaterThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		// if (branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-lessThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		// Map<String, String> serdeConfig =
		// Collections.singletonMap("schema.registry.url",
		// "http://schema-registry:8081");
		// Serde<SimplePojoObjectAvro> avroSerdes = new SpecificAvroSerde<>();
		// avroSerdes.configure(serdeConfig, false);

		// if (branchs.containsKey("branch-greaterThan34") &&
		// branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
		// .flatMapValues((simplePojo) -> {
		// List<SimplePojoObjectAvro> simplePojoObjectAvro = new ArrayList<>();

		// simplePojoObjectAvro.add(SimplePojoObjectAvro.newBuilder()
		// .setName(simplePojo.getName())
		// .setAge(simplePojo.getAge())
		// .setBirthDate(
		// Instant.ofEpochMilli(
		// simplePojo.getBirthDate().getTime()).atZone(ZoneId.systemDefault())
		// .toLocalDate())
		// .build());
		// return simplePojoObjectAvro;
		// })
		// .to("stream-topic-custom-serdes-avro2", Produced.with(Serdes.String(),
		// avroSerdes));

		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// https://toolslick.com/generation/metadata/avro-schema-from-json
		// https://avro.apache.org/docs/1.11.1/getting-started-java/
		// Usefull links to get started

		// ---------------------- 12 -----------------------

		// 12 Exercise
		// using streamBuilder and Topology with custom Serdes, and branching results
		// Used to create separate KStream's with split
		// producing data to another topic using avro.
		// dependendy necessary to import avro Serdes ->
		// io.confluent:kafka-streams-avro-serde
		// <!--
		// https://mvnrepository.com/artifact/io.confluent/kafka-streams-avro-serde
		// -->
		// <dependency>
		// <groupId>io.confluent</groupId>
		// <artifactId>kafka-streams-avro-serde</artifactId>
		// <version>7.2.1</version>
		// </dependency>
		// changing key Using selectKey
		// StreamsBuilder builder = new StreamsBuilder();
		// KStream<String, SimplePojoObject> lines =
		// builder.stream("stream-topic-custom-serdes",
		// Consumed.with(Serdes.String(), new CustomSerdes()));

		// Map<String, KStream<String, SimplePojoObject>> branchs =
		// lines.split(Named.as("branch-")) // if not set a name
		// // fakfka will put
		// // default name
		// .branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
		// .branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
		// .noDefaultBranch();

		// if (branchs.containsKey("branch-greaterThan34")) // concatened as defineda
		// bove
		// branchs.get("branch-greaterThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String,
		// SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		// if (branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-lessThan34")
		// .map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
		// .print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		// Map<String, String> serdeConfig =
		// Collections.singletonMap("schema.registry.url",
		// "http://schema-registry:8081");
		// Serde<SimplePojoObjectAvro> avroSerdes = new SpecificAvroSerde<>();
		// avroSerdes.configure(serdeConfig, false);

		// if (branchs.containsKey("branch-greaterThan34") &&
		// branchs.containsKey("branch-lessThan34"))
		// branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
		// .selectKey((key,value) -> "key-"+value.getAge())
		// .flatMapValues((simplePojo) -> {
		// List<SimplePojoObjectAvro> simplePojoObjectAvro = new ArrayList<>();

		// simplePojoObjectAvro.add(SimplePojoObjectAvro.newBuilder()
		// .setName(simplePojo.getName())
		// .setAge(simplePojo.getAge())
		// .setBirthDate(
		// Instant.ofEpochMilli(
		// simplePojo.getBirthDate().getTime()).atZone(ZoneId.systemDefault())
		// .toLocalDate())
		// .build());
		// return simplePojoObjectAvro;
		// })
		// .to("stream-topic-custom-serdes-avro2", Produced.with(Serdes.String(),
		// avroSerdes));

		// consumer = new KafkaStreams(builder.build(), props);
		// consumer.start();

		// https://toolslick.com/generation/metadata/avro-schema-from-json
		// https://avro.apache.org/docs/1.11.1/getting-started-java/
		// Usefull links to get started

		// -----------------------------------Summary ------------------------------
		// • Filtering data with filter and filterNot
		// • Creating substreams using the branch operator
		// • Combining streams with the merge operator
		// • Performing 1:1 record transformations using map and mapValues
		// • Performing 1:N record transformations using flatMap and flatMapValues
		// • Writing records to output topics using to, through, and repartition
		// • Serializing, deserializing, and reserializing data using custom
		// serializers, deserializers,
		// and Serdes implementations

		// ------------------------ Stateful Operations -------------------------
		// Use case Purpose Operators
		// Joining data Enrich an event with additional information or context
		// that was captured in a separate stream or table
		// • join (inner join)
		// • leftJoin
		// • outerJoin
		// Aggregating data Compute a continuously updating mathematical or
		// combinatorial transformation of related events
		// • aggregate
		// • count
		// • reduce
		// Windowing data Group events that have close temporal proximity • windowedBy

		// ---------------------- 13 -----------------------
		// using Join
		// https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-")) // if not set a name
				// fakfka will put
				// default name
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();
		// change the key
		KStream<String, SimplePojoObject> kStreamGreaterThan24 = branchs.get("branch-greaterThan34")
				.selectKey((k, v) -> k);
		KStream<String, SimplePojoObject> kStreamLessThan24 = branchs.get("branch-lessThan34").selectKey((k, v) -> k);

		// kStreamGreaterThan24
		// .print(Printed.<String,SimplePojoObject>toSysOut().withLabel("kStreamGreaterThan24"));
		// kStreamLessThan24
		// .print(Printed.<String,SimplePojoObject>toSysOut().withLabel("kStreamLessThan24"));

		// //create a joiner
		// ValueJoiner<SimplePojoObject, SimplePojoObject, JoinedObjects> joiner =
		// (left, right) ->
		// new JoinedObjects(left, right);

		kStreamGreaterThan24.join(kStreamLessThan24,
				(left, right) -> new JoinedObjects(left, right), // lambda Joiner, same as create above, but inline
				JoinWindows.of(Duration.ofSeconds(30)),
				StreamJoined.with(Serdes.String(), new CustomSerdes(), new CustomSerdes()))
				.peek((k, v) -> logger.info("key: " + k))
				.print(Printed.<String, JoinedObjects>toSysOut().withLabel("Found a Join"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

	}

}
