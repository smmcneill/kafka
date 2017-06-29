package com.smmcneill.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Factory which generates Kafka ConsumerConnector objects.  Used in conjunction with the KafkaSubscriber class.
 * 
 * Most of the time, users will opt to use the KafkaSubscriber constructor which takes a ConsumerConfig object.  It is more rare
 * to use the IConnectorFactory constructor.
 * 
 * The key driver for the IConnectorFactory interface was to make unit testing more straightforward.
 */
public interface IConnectorFactory {
    /**
     * Creates a new ConsumerConnector object and returns the new connector to the caller
     * @return A new consumer connector
     */
    public ConsumerConnector getConsumerConnector();
    
    /**
     * Provides the caller with the ConsumerConfig object used by this factory
     * @return
     */
    public ConsumerConfig getConsumerConfig();
}
