# Java Kafka subscriber

A thread safe Kafka topic subscriber intended to consume messages from a topic and distribute the messages to a configured message handler

As is recommended in Kafka best practice guide, many high level consumers are created; each of which starts a single thread (aka many connectors, each having one stream).

Alternatively, you can use low level consumers (aka one connector having many streams) by using a constructor which explicitly uses low level consumers.  See usage in the example below.
 
 ```
 // A simple Spring bean which handles message consumption, and includes a method implementation whose 
 // signature looks like: public void method_name(ConsumerConnector, KafkaStream<String, String>, MessageAndMetadata<String, String>)
 @AutoWired
 TopicConsumer mTopicConsumer;
 
 @Bean
 public KafkaSubscriber kafkaConsumer(){
	if(!mEnableKafkaConsumer){
		gLogger.debug("Kafka consumer disabled");
		return null;
	}

	return new KafkaSubscriber(5, "topic_name", mTopicConsumer, new ConsumerConfig(new Properties() {
		{
			put("zookeeper.connect", "localhost:2181");
			put("group.id", "group_name");
			put("zookeeper.session.timeout.ms", 2000);
			put("zookeeper.sync.time.ms",  3000);
			put("auto.commit.enable", "false");
			put("auto.offset.reset", "smallest");
			// We don't store offsets, but be consistent with other consumers
			put("offsets.storage", "kafka");
		}
	}));
}

...

@Component
public class TopicConsumer{
	@AutoWired
	x, y, z // Since this is a spring bean, autowire away
    
	public void consumeMessage(ConsumerConnector connector, KafkaStream<String, String> stream, MessageAndMetadata<String, String> message){
		Object s = JsonUtil.unmarshal(message.message(), Object.class);
		...
		connector.commitOffsets();
		...
	}
}

```