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
import javax.websocket.Session;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.BranchedKStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.internals.MaterializedInternal;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@SpringBootApplication
@EnableScheduling
@Configuration
public class KafkaTrainingApplication {

	Logger logger = Logger.getLogger(this.toString());

	Properties props = new Properties();
	private KafkaStreams consumer;

	public KafkaStreams getConsumer() {
		return consumer;
	}

	public static void main(String[] args) {

		SpringApplication.run(KafkaTrainingApplication.class, args);
	}

	@PostConstruct
	void doingSetups() {
		logger.info("Doing setups..");
		this.props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092,kafka-2:29092,kafka-3:39092");
		this.props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-consumer-7");
		this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		this.props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		this.props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		this.props.put("schema.registry.url", "http://schema-registry:8081");
		this.props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "app:8080");

		// ------------------------ Statefuless Operations -------------------------
		// ---------------------- 1 -----------------------
		// ---> Simple Consumer String
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> lines = builder.stream("stream-topic");
		lines.foreach((key, value) -> {
			logger.info("Hello from simple stream consumer: Key: " + key + " value: "
					+ value);
		});
		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 2 -----------------------
		// ---> Using Topology of our own
		// ---> Custom Processor MyProcessor::new
		Topology topology = new Topology();
		topology.addSource("SimpleSource", "stream-topic");
		topology.addProcessor("SimpleProcessor", MyProcessor::new, "SimpleSource");
		consumer = new KafkaStreams(topology, props);
		consumer.start();

		// ---------------------- 3 -----------------------
		// ---> using streamBuilder and .print(Printed...
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> lines = builder.stream("stream-topic");
		lines.print(Printed.<String, String>toSysOut().withLabel("tweets-stream"));
		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 4 -----------------------
		// --->using custom Serdes, CustomSerdes.java
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));
		lines.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("SimplePojoObject-stream"));
		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 5 -----------------------

		// --->using filtering in lambda functions
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));
		lines.filter((key, simplePojoObject) -> simplePojoObject.getAge() >= 34)
				.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("SimplePojoObject-stream"));
		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 6 -----------------------
		// --->using filteringNot in lambda functions
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));
		lines.filterNot((key, simplePojoObject) -> simplePojoObject.getAge() >= 34)
				.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("SimplePojoObject-stream"));
		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 7 -----------------------
		// --->using split to create separate KStreams
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-")) // if not set a name
				// kafka will put
				// defaultname
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		if (branchs.containsKey("branch-greaterThan34")) // concatened as defined above
			branchs.get("branch-greaterThan34")
					.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		if (branchs.containsKey("branch-lessThan34"))
			branchs.get("branch-lessThan34")
					.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 8 -----------------------
		// ---> using Map to rekey or change data in the message, using KeyValue.pair()
		// of the library
		// ---> there is also a mapValues, used to map any field of the data
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		if (branchs.containsKey("branch-greaterThan34"))
			branchs.get("branch-greaterThan34")
					.map((key, value) -> KeyValue.pair(key + "-" + value.getAge(), value))
					.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("greaterThan34"));

		if (branchs.containsKey("branch-lessThan34"))
			branchs.get("branch-lessThan34")
					.mapValues((value) -> {
						value.setAge(value.getAge() + 1);
						return value;
					})
					.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("lessThan34"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 9 -----------------------
		// ---> using Merge to merge KafkaStreams.
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		if (branchs.containsKey("branch-greaterThan34") &&
				branchs.containsKey("branch-lessThan34"))
			branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
					.print(Printed.<String, SimplePojoObject>toSysOut().withLabel("Merged"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 10 -----------------------
		// ---> using flatMap when working with multiple arrays object, and you want to
		// flat that array into single array object.
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		List<Integer> ages = new ArrayList<>();

		if (branchs.containsKey("branch-greaterThan34") &&
				branchs.containsKey("branch-lessThan34"))
			branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
					.flatMapValues((simplePojo) -> {
						ages.add(simplePojo.getAge());
						logger.info("ages are:" + ages);
						return ages;
					}).foreach((key, v) -> {
						logger.info("key: " + key);
						logger.info("v: " + v);
					});

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 11 -----------------------
		// ---> using producing data to another topic using avro.
		// ---> dependendy necessary to import avro Serdes ->
		// io.confluent:kafka-streams-avro-serde
		// <!--
		// https://mvnrepository.com/artifact/io.confluent/kafka-streams-avro-serde
		// -->
		// <dependency>
		// <groupId>io.confluent</groupId>
		// <artifactId>kafka-streams-avro-serde</artifactId>
		// <version>7.2.1</version>
		// </dependency>
		// https://toolslick.com/generation/metadata/avro-schema-from-json
		// https://avro.apache.org/docs/1.11.1/getting-started-java/
		// Usefull links to get started
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
				"http://schema-registry:8081");
		Serde<SimplePojoObjectAvro> avroSerdes = new SpecificAvroSerde<>();
		avroSerdes.configure(serdeConfig, false);

		if (branchs.containsKey("branch-greaterThan34") &&
				branchs.containsKey("branch-lessThan34"))
			branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
					.flatMapValues((simplePojo) -> {
						List<SimplePojoObjectAvro> simplePojoObjectAvro = new ArrayList<>();

						simplePojoObjectAvro.add(SimplePojoObjectAvro.newBuilder()
								.setName(simplePojo.getName())
								.setAge(simplePojo.getAge())
								.setBirthDate(
										Instant.ofEpochMilli(
												simplePojo.getBirthDate().getTime()).atZone(ZoneId.systemDefault())
												.toLocalDate())
								.build());
						return simplePojoObjectAvro;
					})
					.to("stream-topic-custom-serdes-avro2",
							Produced.with(Serdes.String(), avroSerdes));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 12 -----------------------
		// ---> changing key Using selectKey, great when message has no key
		// ---> Warning: Since kafka message are immutable, using the selectKey method
		// will cause kafka to
		// create a new topic for storing messages this new key.
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
				"http://schema-registry:8081");
		Serde<SimplePojoObjectAvro> avroSerdes = new SpecificAvroSerde<>();
		avroSerdes.configure(serdeConfig, false);

		if (branchs.containsKey("branch-greaterThan34") &&
				branchs.containsKey("branch-lessThan34"))
			branchs.get("branch-greaterThan34").merge(branchs.get("branch-lessThan34"))
					.selectKey((key, value) -> "key-" + value.getAge())
					.flatMapValues((simplePojo) -> {
						List<SimplePojoObjectAvro> simplePojoObjectAvro = new ArrayList<>();

						simplePojoObjectAvro.add(SimplePojoObjectAvro.newBuilder()
								.setName(simplePojo.getName())
								.setAge(simplePojo.getAge())
								.setBirthDate(
										Instant.ofEpochMilli(
												simplePojo.getBirthDate().getTime()).atZone(ZoneId.systemDefault())
												.toLocalDate())
								.build());
						return simplePojoObjectAvro;
					})
					.to("stream-topic-custom-serdes-avro2", Produced.with(Serdes.String(),
							avroSerdes));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

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
		// ---> using Join
		// ---> using peek to log the messages.
		// https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> kStreamGreaterThan24 = branchs.get("branch-greaterThan34");
		KStream<String, SimplePojoObject> kStreamLessThan24 = branchs.get("branch-lessThan34");

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

		// ---------------------- 14 -----------------------
		// using Join, and Group By to count
		// ---> Warning: Since kafka message are immutable, using the groupBy method
		// will cause kafka to
		// create a new topic for storing messages this new key.
		// if your messages has a key, it is strongly recommended to use the groupByKey
		// method.
		// https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();
		KStream<String, SimplePojoObject> kStreamGreaterThan24 = branchs.get("branch-greaterThan34");
		KStream<String, SimplePojoObject> kStreamLessThan24 = branchs.get("branch-lessThan34");

		kStreamGreaterThan24.join(kStreamLessThan24,
				(left, right) -> new JoinedObjects(left, right),
				JoinWindows.of(Duration.ofSeconds(30)),
				StreamJoined.with(Serdes.String(), new CustomSerdes(), new CustomSerdes()))
				// with GroupBy we can select a new key for the data, or we can use
				// GroupByKey to select the key thats ships with the original data.
				// A complex type must implement a new Serdes
				.groupBy((k, v) -> k, Grouped.with(Serdes.String(), new CustomSerdesForJoinedObjects()))
				.count().toStream() // count and convert back to stream
				.print(Printed.<String, Long>toSysOut().withLabel("Joins"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// // ---------------------- 15 -----------------------
		// ---> using aggregate
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		lines
				.groupByKey()
				.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
				.aggregate(
						() -> 0.0,
						(key, joinedObj, newValue) -> {
							logger.info("===========");
							logger.info("Key: " + key);
							logger.info("key1.getAge(): " + joinedObj.getAge());
							logger.info("newValue:" + newValue);
							return newValue + 1.0;
						},
						Materialized.with(Serdes.String(), Serdes.Double()))
				.toStream()
				.map((k, v) -> KeyValue.pair(k.key(), v))
				.peek((k, v) -> logger.info("key= " + k + " value= " + v))
				.print(Printed.<String, Double>toSysOut().withLabel("Joins"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// // ---------------------- 16 -----------------------
		// ---> using aggregate with class, must be created a Serializer/Deserializer.
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> kStreamGreaterThan24 = branchs.get("branch-greaterThan34");
		KStream<String, SimplePojoObject> kStreamLessThan24 = branchs.get("branch-lessThan34");

		kStreamGreaterThan24.join(kStreamLessThan24,
				(left, right) -> new JoinedObjects(left, right),
				JoinWindows.of(Duration.ofSeconds(10)),
				StreamJoined.with(Serdes.String(), new CustomSerdes(), new CustomSerdes()))
				.groupBy((k, v) -> k, Grouped.with(Serdes.String(), new CustomSerdesForJoinedObjects()))
				.aggregate(
						InitializerForAggregate::new,
						(key, value, initializedVar) -> initializedVar.addNewData(key, value),
						Materialized.with(Serdes.String(), new CustomSerDesForInitializerForAggregate()))
				.toStream()
				.print(Printed.<String, InitializerForAggregate>toSysOut().withLabel("Joins"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 17 -----------------------
		// ---> Using a custom state store materialized by name with aggregate.
		// ---> Using Stream query, to query state store materialized.

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new CustomSerdes()));

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> value.getAge() >= 34, Branched.as("greaterThan34"))
				.branch((key, value) -> value.getAge() < 34, Branched.as("lessThan34"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> kStreamGreaterThan24 = branchs.get("branch-greaterThan34");
		KStream<String, SimplePojoObject> kStreamLessThan24 = branchs.get("branch-lessThan34");

		kStreamGreaterThan24.join(kStreamLessThan24,
				(left, right) -> new JoinedObjects(left, right),
				JoinWindows.of(Duration.ofSeconds(60)),
				StreamJoined.with(Serdes.String(), new CustomSerdes(), new CustomSerdes()))
				.groupBy((k, v) -> k, Grouped.with(Serdes.String(), new CustomSerdesForJoinedObjects()))
				.aggregate(
						InitializerForAggregate::new,
						(key, value, initializedVar) -> initializedVar.addNewData(key, value),
						Materialized
								.<String, InitializerForAggregate, KeyValueStore<Bytes, byte[]>>as(
										"state-store-for-InitializerForAggregate")
								.withKeySerde(Serdes.String())
								.withValueSerde(new CustomSerDesForInitializerForAggregate()))
				.toStream()
				.print(Printed.<String, InitializerForAggregate>toSysOut().withLabel("Joins"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// this will throw an exception, because the state store is not
		// initialized yet... take some time.
		InitializerForAggregate object = (InitializerForAggregate) consumer.store(
				StoreQueryParameters.fromNameAndType("state-store-for-InitializerForAggregate",
						QueryableStoreTypes.keyValueStore()))
				.get("random1");
		logger.info("object: " + object.toString());

		// ---------------------- 18 -----------------------
		// ---> Custom TimeStamp extractor
		// ---> tumbling Windows
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new SerDesForSimplePojoObject())
						.withTimestampExtractor(new MessageTimestampExtractor())); // customtime-stamp extractor

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-"))
				.branch((key, value) -> key.equals("random1") || key.equals("random2"),
						Branched.as("keys1-2"))
				.branch((key, value) -> key.equals("random3") || key.equals("random4"),
						Branched.as("keys2-4"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> keys1t2 = branchs.get("branch-keys1-2");
		KStream<String, SimplePojoObject> keys3t4 = branchs.get("branch-keys2-4");

		keys1t2.groupByKey()
				// tumbling window by 30 seconds and 10 seconds of grace.
				.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30),
						Duration.ofSeconds(10)))
				.count(Materialized.as("1-2"))
				.toStream()
				.map((k, v) -> KeyValue.pair(k.key(), v))
				.print(Printed.<String, Long>toSysOut().withLabel("keys1-2"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 19 -----------------------
		// ---> Custom TimeStamp extractor
		// ---> hopping Windows
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new SerDesForSimplePojoObject())
						.withTimestampExtractor(new MessageTimestampExtractor())); // customtime-stamp extractor

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-")) // if not set a name
				.branch((key, value) -> key.equals("random1") || key.equals("random2"),
						Branched.as("keys1-2"))
				.branch((key, value) -> key.equals("random3") || key.equals("random4"),
						Branched.as("keys2-4"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> keys1t2 = branchs.get("branch-keys1-2");
		KStream<String, SimplePojoObject> keys3t4 = branchs.get("branch-keys2-4");

		keys1t2.groupByKey()
				// hopping window by 30 seconds and 10 seconds of grace.
				.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(30),
						Duration.ofSeconds(10))
						.advanceBy(Duration.ofSeconds(10)))
				.count(Materialized.as("1-2"))
				.toStream()
				.print(Printed.<Windowed<String>, Long>toSysOut().withLabel("keys1-2"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 20 -----------------------
		// ---> Custom TimeStamp extractor
		// ---> Session Windows
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new SerDesForSimplePojoObject())
						.withTimestampExtractor(new MessageTimestampExtractor())); // customtime-stamp extractor

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-")) // if not set a name
				.branch((key, value) -> key.equals("random1") || key.equals("random2"),
						Branched.as("keys1-2"))
				.branch((key, value) -> key.equals("random3") || key.equals("random4"),
						Branched.as("keys2-4"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> keys1t2 = branchs.get("branch-keys1-2");
		KStream<String, SimplePojoObject> keys3t4 = branchs.get("branch-keys2-4");

		keys1t2.groupByKey()
				// session window by 30 seconds and 10 seconds of grace.
				// Session window only shows de results when production stops, in this stops for
				// 10 seconds.
				.windowedBy(SessionWindows.ofInactivityGapAndGrace(Duration.ofSeconds(5),
						Duration.ofSeconds(10)))
				.count(Materialized.as("1-2"))
				.toStream()
				.print(Printed.<Windowed<String>, Long>toSysOut().withLabel("keys1-2"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 21 -----------------------
		// ---> Custom TimeStamp extractor
		// ---> Sliding window
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new SerDesForSimplePojoObject())
						.withTimestampExtractor(new MessageTimestampExtractor())); // customtime-stamp extractor

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-")) // if not set a name
				.branch((key, value) -> key.equals("random1") || key.equals("random2"),
						Branched.as("keys1-2"))
				.branch((key, value) -> key.equals("random3") || key.equals("random4"),
						Branched.as("keys2-4"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> keys1t2 = branchs.get("branch-keys1-2");
		KStream<String, SimplePojoObject> keys3t4 = branchs.get("branch-keys2-4");

		keys1t2.groupByKey()
				// Sliding of 10secontext
				.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10),
						Duration.ofSeconds(5)))
				.count(Materialized.as("1-2-Sliding"))
				.toStream()
				.print(Printed.<Windowed<String>, Long>toSysOut().withLabel("keys1-2"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 22 -----------------------
		// ---> Custom TimeStamp extractor
		// ---> TumbingWindows window
		// ---> Supress Operator
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new SerDesForSimplePojoObject())
						.withTimestampExtractor(new MessageTimestampExtractor())); // customtime-stamp extractor

		Map<String, KStream<String, SimplePojoObject>> branchs = lines.split(Named.as("branch-")) // if not set a name
				.branch((key, value) -> key.equals("random1") || key.equals("random2"),
						Branched.as("keys1-2"))
				.branch((key, value) -> key.equals("random3") || key.equals("random4"),
						Branched.as("keys2-4"))
				.noDefaultBranch();

		KStream<String, SimplePojoObject> keys1t2 = branchs.get("branch-keys1-2");
		KStream<String, SimplePojoObject> keys3t4 = branchs.get("branch-keys2-4");

		keys1t2.groupByKey()
				// TumbingWindows of 10secontext
				.windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10),
						Duration.ofSeconds(5)))
				.count(Materialized.as("1-2-TumbingWindows"))
				// suppress operator, will hold the data until the windows time is reached.
				.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(15),
						BufferConfig.unbounded().emitEarlyWhenFull()))
				.toStream()

				.map((windowKey, value) -> KeyValue.pair(windowKey.key(), value))// rekey, because itens comes with
																					// different key
				.print(Printed.<String, Long>toSysOut().withLabel("keys1-2-TumbingWindows-supressed"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// ---------------------- 23 -----------------------
		// ---> Supress Operator
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, SimplePojoObject> lines = builder.stream("stream-topic-custom-serdes",
				Consumed.with(Serdes.String(), new SerDesForSimplePojoObject())
						.withTimestampExtractor(new MessageTimestampExtractor()));

		lines
				.peek((k, v) -> logger.info("key= " + k + " value= " + v))
				.groupByKey()
				.count()
				// suppress operator, will hold the data until the windows time is reached.
				.suppress(
						Suppressed.untilTimeLimit(Duration.ofMillis(5),
								BufferConfig.unbounded().emitEarlyWhenFull()))
				.toStream()
				.map((windowKey, value) -> KeyValue.pair(windowKey, value))
				.print(Printed.<String, Long>toSysOut().withLabel("keys1-2-TumbingWindows-supressed"));

		consumer = new KafkaStreams(builder.build(), props);
		consumer.start();

		// --------------------- end --------------------------------

		Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));

	}

}
