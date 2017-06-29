package com.smmcneill.kafka;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.lang3.BooleanUtils.xor;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;


/**
 * A thread safe Kafka topic subscriber intended to consume messages from a topic and distribute the messages to a configured
 * message handler
 * 
 * As is recommended in the following best practice guide, many high level consumers are created; each of which starts a
 * single thread (aka many connectors, each having one stream).
 * 
 * Alternatively, you can use low level consumers (aka one connector having many streams) by using a constructor which explicitly uses low level consumers.
 * See usage in the example below.
 * 
 * An example of use:
 * 
 *  // A simple Spring bean which handles message consumption, and includes a method implementation whose 
 *  // signature looks like: public void method_name(ConsumerConnector, KafkaStream<String, String>, MessageAndMetadata<String, String>)
 *  @AutoWired
 *  TopicConsumer mTopicConsumer;
 *  
 *  @Bean
 *  public KafkaSubscriber kafkaConsumer(){
 *      if(!mEnableKafkaConsumer){
 *          gLogger.debug("Kafka consumer disabled");
 *          return null;
 *      }
 *      
 *      return new KafkaSubscriber(5, "topic_name", mTopicConsumer, new ConsumerConfig(new Properties() {
 *          {
 *              put("zookeeper.connect", "localhost:2181");
 *              put("group.id", "group_name");
 *              put("zookeeper.session.timeout.ms", 2000);
 *              put("zookeeper.sync.time.ms",  3000);
 *              put("auto.commit.enable", "false");
 *              put("auto.offset.reset", "smallest");
 *              // We don't store offsets, but be consistent with other consumers
 *              put("offsets.storage", "kafka");
 *          }
 *      }));
 *  }
 *  
 *  ...
 *  
 *  @Component
 *  public class TopicConsumer{
 *      @AutoWired
 *      x, y, z // Since this is a spring bean, autowire away
 *  
 *      public void consumeMessage(ConsumerConnector connector, KafkaStream<String, String> stream, MessageAndMetadata<String, String> message){
 *          Object s = JsonUtil.unmarshal(message.message(), Object.class);
 *          ...
 *          connector.commitOffsets();
 *          ...
 *      }
 *      
 *      ...
 */
public class KafkaSubscriber {
    private static long mRetryHesitationMs = TimeUnit.SECONDS.toMillis(10);
    
    private final static Logger gLogger = LoggerFactory.getLogger(KafkaSubscriber.class);
    
    private int mHighLevelThreadCount;
    
    private int mStreamCount;

    private String mTopicName;
    
    private TopicFilter mTopicFilter;
    
    private IConnectorFactory mConnectorFactory;

    private ExecutorService mExecutor;
    
    private Thread mStartupThread;
    
    private List<ConnectorAndStreams> mConnectorsAndStreams;
    
    /**
     * Constructs a subscriber which handles messages published to a specified topic.  By default, high level
     * consumers are used (see class level javadoc for details).  Also, connector creation is blocking by default.
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicName The name of the topic from which to consume messages
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param consumerConfig Kafka consumer configuration
     */
    public KafkaSubscriber(int consumerCount, String topicName, Object bean, final ConsumerConfig consumerConfig){
        this(consumerCount, topicName, bean, consumerConfig, false, false);
    }
    
    /**
     * Constructs a subscriber which handles messages published to the set of topics defined by the supplied
     * TopicFilter.  By default, high level consumers are used (see class level javadoc for details).  
     * Also, connector creation is blocking by default.
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicFilter The filter describing the set of topics to consume from
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param consumerConfig Kafka consumer configuration
     */
    public KafkaSubscriber(int consumerCount, TopicFilter topicFilter, Object bean, final ConsumerConfig consumerConfig){
        this(consumerCount, topicFilter, bean, consumerConfig, false, false);
    }

    
    /**
     * Constructs a subscriber which handles messages published to a specified topic.  By default, high level
     * consumers are used (see class level javadoc for details).  Also, connector creation is blocking by default.
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicName The name of the topic from which to consume messages
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param connectorFactory The connector factory that produces ConsumerConnectors
     */
    public KafkaSubscriber(int consumerCount, String topicName, Object bean, IConnectorFactory connectorFactory) {
        this(consumerCount, topicName, null, bean, connectorFactory, false, false);
    }
    
    /**
     * Constructs a subscriber which handles messages published to a specified topic
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicName The name of the topic from which to consume messages
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param consumerConfig Kafka consumer configuration
     * @param useLowLevelConsumers True when the subscriber should use low level consumers (see class level javadoc for details)
     * @param startupInBackground True when the subscriber should startup connection creation effort in the background (non-blocking)
     */
    public KafkaSubscriber(int consumerCount, String topicName, Object bean, final ConsumerConfig consumerConfig, boolean useLowLevelConsumers, boolean startupInBackground) {
        this(consumerCount, topicName, null, bean, consumerConfig, useLowLevelConsumers, startupInBackground);
    }
    
    /**
     * Constructs a subscriber which handles messages published to the set of topics defined by the supplied
     * TopicFilter.
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicFilter The filter which defines the set of topics from which to consume messages
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param consumerConfig Kafka consumer configuration
     * @param useLowLevelConsumers True when the subscriber should use low level consumers (see class level javadoc for details)
     * @param startupInBackground True when the subscriber should startup connection creation effort in the background (non-blocking)
     */
    public KafkaSubscriber(int consumerCount, TopicFilter topicFilter, Object bean, final ConsumerConfig consumerConfig, boolean useLowLevelConsumers, boolean startupInBackground) {
        this(consumerCount, null, topicFilter, bean, consumerConfig, useLowLevelConsumers, startupInBackground);
    }
    
    /**
     * Constructs a subscriber which handles messages published to a specified topic or set of topics,
     * using a default IConnectorFactory.
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicName The name of the topic from which to consume messages. Exactly one of topicName, 
     *                  topicFilter should be non null.
     * @param topicFilter The filter which defines the set of topics from which to consume messages.
     *                  Exactly one of topicName, topicFilter should be non null.
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param consumerConfig Kafka consumer configuration
     * @param useLowLevelConsumers True when the subscriber should use low level consumers (see class level javadoc for details)
     * @param startupInBackground True when the subscriber should startup connection creation effort in the background (non-blocking)
     */
    private KafkaSubscriber(int consumerCount, String topicName, TopicFilter topicFilter, Object bean, final ConsumerConfig consumerConfig, boolean useLowLevelConsumers, boolean startupInBackground) {
        this(consumerCount, topicName, topicFilter, bean, new IConnectorFactory() {
            @Override
            public ConsumerConnector getConsumerConnector() {
                return Consumer.createJavaConsumerConnector(consumerConfig);
            }
            
            @Override
            public ConsumerConfig getConsumerConfig(){
                return consumerConfig;
            }
        }, useLowLevelConsumers, startupInBackground);
    }

    /**
     * Constructs a subscriber which handles messages published to a specified topic or set of topics.
     * 
     * @param consumerCount The number of consumers to create in order to handle message processing
     * @param topicName The name of the topic from which to consume messages. Exactly one of topicName, 
     *                  topicFilter should be non null.
     * @param topicFilter The filter which defines the set of topics from which to consume messages
     *                  Exactly one of topicName, topicFilter should be non null.
     * @param bean The object which will be responsible for receiving and reacting to consumed messages
     * @param connectorFactory The connector factory that produces ConsumerConnectors
     * @param useLowLevelConsumers True when the subscriber should use low level consumers (see class level javadoc for details)
     * @param startupInBackground True when the subscriber should startup connection creation effort in the background (non-blocking)
     */
     KafkaSubscriber(int consumerCount, String topicName, TopicFilter topicFilter, Object bean, IConnectorFactory connectorFactory, 
            boolean useLowLevelConsumers, boolean startupInBackground) {
        checkArgument(consumerCount > 0);
        
        // Must specify exactly one of topicName, topicFilter
        checkArgument(xor(new boolean[]{StringUtils.isBlank(topicName), (null == topicFilter)}));
        checkArgument(null != bean);
        checkArgument(null != connectorFactory);
        
        ConsumerConfig config = connectorFactory.getConsumerConfig();
        
        checkArgument(null != config);
        
        if(useLowLevelConsumers){
            mStreamCount = consumerCount;
            mHighLevelThreadCount = 1;
        }else{
            mStreamCount = 1;
            mHighLevelThreadCount = consumerCount;
        }
        
        mTopicName = topicName;
        mTopicFilter = topicFilter;
        mConnectorFactory = connectorFactory;
        mConnectorsAndStreams = new ArrayList<>(mHighLevelThreadCount);
        
        // Get the method that handles message consumption
        Method targetMethod = getMethod(bean);
        
        if(startupInBackground){
            mStartupThread = new Thread(() -> {
                init(bean, targetMethod);
            });
            
            mStartupThread.start();
        }else{
            init(bean, targetMethod);
        }
    }
    
    /**
     * 
     */
    private void init(Object bean, Method targetMethod) {
        gLogger.info("Topic {}: creating {} consumer instances streams {}", mTopicName, 
                mHighLevelThreadCount, mStreamCount);
        // Attempt to create the consumer instances
        for(int count = 0;count < mHighLevelThreadCount;){
            // Check for interrupt if running in background
            if(null != mStartupThread && Thread.interrupted()){
                gLogger.info("Topic {}: startup interrupted.  Shutting down subscriber.", mTopicName);
                return;
            }
            
            try{
                // Attempt to create Kafka collateral (consumer connector and stream(s))
                createKafkaCollateral();
                
                gLogger.debug("Topic {}: successfully created consumer {}", mTopicName, count);
                
                count++; // If we were able to successfully create the collateral, increment
            }catch(Exception e){
                gLogger.error("Topic {}: setup unable to connect to broker, retrying in {} seconds", 
                        mTopicName, TimeUnit.MILLISECONDS.toSeconds(mRetryHesitationMs), e);
                
                try {
                    // Give it some hesitation, then try again
                    TimeUnit.MILLISECONDS.sleep(mRetryHesitationMs);
                } catch (InterruptedException ex) {
                    gLogger.warn("Topic {}: interrupted while waiting to retry...", mTopicName, ex);
                    return;
                }
            }
        }
        
        mExecutor = Executors.newFixedThreadPool(mHighLevelThreadCount * mStreamCount); // One or the other value is 1 
        
        for(ConnectorAndStreams connectorAndStreams : mConnectorsAndStreams){
            ConsumerConnector consumerConnector = connectorAndStreams.mConsumerConnector;
            
            for(KafkaStream<String, String> kStream : connectorAndStreams.mKafkaStreams){
                mExecutor.submit(() -> {
                    gLogger.debug("Topic {}: consumer thread {} running", mTopicName, Thread.currentThread().getName());
                    for (
                        ConsumerIterator<String, String> consumerIterator = kStream.iterator(); 
                        consumerIterator.hasNext();
                    ) {
                        try {
                            MessageAndMetadata<String, String> messageAndMetadata = consumerIterator.next();

                            // Call the bean's target method, passing the stream collateral
                            targetMethod.invoke(bean, consumerConnector, kStream, messageAndMetadata);
                        } catch (Throwable e) {
                            gLogger.error("Topic {}: an error occurred while processing Kafka message", mTopicName, e);
                        }
                    }
                    gLogger.debug("Topic {}: consumer thread {} exiting", mTopicName, Thread.currentThread().getName());
                });
            }
        }
    }

    /**
     * 
     */
    @PreDestroy
    public void cleanup() {
        if(null != mStartupThread){
            // Interrupt startup thread
            mStartupThread.interrupt();
            
            try {
                // Current thread pauses waiting for startup to terminate
                mStartupThread.join(5000);
            } catch (InterruptedException e) {
                gLogger.error("An interrupt occurred while waiting to terminate startup thread", e);
            }
        }
        
        for(ConnectorAndStreams consumerConnector : mConnectorsAndStreams){
            consumerConnector.mConsumerConnector.shutdown();
        }
        
        if(null != mExecutor){
            mExecutor.shutdown();
        }
    }

    /**
     * Create consumer connector and message stream(s).  For high level config, 1 connector is created and one stream
     * is created.  For low level config, 1 connector is created and many streams are created.
     * 
     */
    private void createKafkaCollateral() {
        ConsumerConnector consumerConnector = mConnectorFactory.getConsumerConnector();

        if (mTopicName != null) {
            Map<String, List<KafkaStream<String, String>>> messageStreams = consumerConnector.createMessageStreams(
                    ImmutableMap.of(mTopicName, mStreamCount), 
                    new StringDecoder(null), 
                    new StringDecoder(null)
                    );
            mConnectorsAndStreams.add(new ConnectorAndStreams(consumerConnector, messageStreams.get(mTopicName)));
        } else {
            checkArgument(mTopicFilter != null);
            List<KafkaStream<String, String>> streams = consumerConnector.createMessageStreamsByFilter(mTopicFilter, mStreamCount,
                    new StringDecoder(null), 
                    new StringDecoder(null)
                    );
            mConnectorsAndStreams.add(new ConnectorAndStreams(consumerConnector, streams));
       }

    }

    /**
     * This class expects that a handler bean will be injected into the constructor. This bean
     * is required to include an instance method with a specific signature:
     * 
     * public void method_name(ConsumerConnector, KafkaStream<String, String>, MessageAndMetadata<String, String>)
     * 
     * @param bean The bean that handles message handling
     * @return The target method of the bean
     */
    private Method getMethod(Object bean) {
        for (Method method : bean.getClass().getMethods()) {
            Class<?>[] types = method.getParameterTypes();
            
            if (
                3 == method.getParameterCount()
                && types[0].isAssignableFrom(ConsumerConnector.class)
                && types[1].isAssignableFrom(KafkaStream.class)
                && types[2].isAssignableFrom(MessageAndMetadata.class)                
                && method.getReturnType() == void.class
            ) {
                return method;
            }
        }

        gLogger.error("There is no handler for the topic: {}", mTopicName);
        
        throw new RuntimeException("There is no handler method matching the signature: public void method_name"
                + "(ConsumerConnector, KafkaStream<String, String>, MessageAndMetadata<String, String>)");
    }
    
    /**
     * 
     */
    static class ConnectorAndStreams{
        public ConsumerConnector mConsumerConnector;
        public List<KafkaStream<String, String>> mKafkaStreams;
        
        /**
         * @param consumerConnector
         * @param kafkaStreams
         */
        public ConnectorAndStreams(ConsumerConnector consumerConnector,
                List<KafkaStream<String, String>> kafkaStreams) {
            checkArgument(null != consumerConnector);
            checkArgument(null != kafkaStreams);
            
            mConsumerConnector = consumerConnector;
            mKafkaStreams = kafkaStreams;
        }
    }
    
    /**
     * Standalone tester
     * @param args
     */
    @SuppressWarnings("serial")
    public static void main(String[] args) {
        TestConsumer consumer = new TestConsumer();
        
        new KafkaSubscriber(5, "xxx", consumer, new ConsumerConfig(new Properties() {
            {
                put("zookeeper.connect", "localhost:2181");
                put("group.id", "test");
                put("zookeeper.session.timeout.ms", "" + 3000);
                put("zookeeper.sync.time.ms",  "" + 2000);
                put("auto.commit.enable", "false");
                put("auto.offset.reset", "smallest");
                put("offsets.storage", "kafka");
            }
        }));
    }
    
    /**
     * 
     */
    private static class TestConsumer{
        /**
         * 
         * @param mm
         */
        @SuppressWarnings("unused")
        public void consume(ConsumerConnector connector, KafkaStream<String, String> stream, MessageAndMetadata<String, String> mm){
            System.out.println(String.format("[%s] [%s] [%s] [%s]", Thread.currentThread().getName(), connector, stream, mm.message()));
        }
    }
}