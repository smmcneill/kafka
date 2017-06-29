package com.smmcneill.kafka;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.Mock;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.smmcneill.kafka.KafkaSubscriber.ConnectorAndStreams;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * 
 */
@RunWith(MockitoJUnitRunner.class)
public class KafkaSubscriberTest {
    @Mock
    private IConnectorFactory mConnectorFactory;
    
    @Mock(answer=Answers.RETURNS_DEEP_STUBS)
    private ConsumerConfig mConsumerConfig;
    
    @Mock
    private ConsumerConnector mConsumerConnector;
    
    /**
     * 
     */
    @Test(expected=IllegalArgumentException.class)
    public void invalidConsumerCount(){
        new KafkaSubscriber(-3, "topic", new TestConsumer(), new ConsumerConfig(new Properties()));
    }
    
    /**
     * 
     */
    @Test(expected=IllegalArgumentException.class)
    public void nullTopicName(){
        new KafkaSubscriber(5, (String)null, new TestConsumer(), new ConsumerConfig(new Properties()));
    }
    
    /**
     * 
     */
    @Test(expected=IllegalArgumentException.class)
    public void emptyTopicName(){
        new KafkaSubscriber(5, "   ", new TestConsumer(), new ConsumerConfig(new Properties()));
    }
    
    /**
     * 
     */
    @Test(expected=RuntimeException.class)
    public void badConsumer(){
        // Consumer doesn't have a method of appropriate signature
        new KafkaSubscriber(5, "topic", new Object(), new ConsumerConfig(new Properties()));
    }
    
    /**
     * 
     */
    @Test(expected=IllegalArgumentException.class)
    public void noConfig(){
        new KafkaSubscriber(5, "topic", new TestConsumer(), (ConsumerConfig)null);
    }
    
    /**
     * 
     */
    @Test(expected=IllegalArgumentException.class)
    public void noConnectorFactory(){
        new KafkaSubscriber(5, "topic", new TestConsumer(), (IConnectorFactory)null);
    }
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void highLevelConsumers() throws Exception{
        when(mConnectorFactory.getConsumerConfig()).thenReturn(mConsumerConfig);
        when(mConnectorFactory.getConsumerConnector()).thenReturn(mConsumerConnector);
        when(mConsumerConnector.createMessageStreams(anyMap(), anyObject(), anyObject()))
            .thenReturn(ImmutableMap.of("topic", ImmutableList.of()));
        
        KafkaSubscriber subscriber = new KafkaSubscriber(5, "topic", new TestConsumer(), mConnectorFactory);
        
        List<ConnectorAndStreams> connectorsAndStreams = 
                (List<ConnectorAndStreams>)ReflectionTestUtils.getField(subscriber, "mConnectorsAndStreams");
        
        TimeUnit.SECONDS.sleep(1);  // Thread creation hesitation
        
        assertTrue(null != connectorsAndStreams);
        assertTrue(5 == connectorsAndStreams.size());
        
        verify(mConnectorFactory).getConsumerConfig();
        verify(mConnectorFactory, times(5)).getConsumerConnector();
        verify(mConsumerConnector, times(5)).createMessageStreams(anyMap(), anyObject(), anyObject());
    }
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void lowLevelConsumers(){
        when(mConnectorFactory.getConsumerConfig()).thenReturn(mConsumerConfig);
        when(mConnectorFactory.getConsumerConnector()).thenReturn(mConsumerConnector);
        when(mConsumerConnector.createMessageStreams(anyMap(), anyObject(), anyObject()))
            .thenReturn(ImmutableMap.of("topic", ImmutableList.of()));
        
        KafkaSubscriber subscriber = new KafkaSubscriber(5, "topic", null, new TestConsumer(), mConnectorFactory, true, false);
        
        List<ConnectorAndStreams> connectorsAndStreams = 
                (List<ConnectorAndStreams>)ReflectionTestUtils.getField(subscriber, "mConnectorsAndStreams");
        
        assertTrue(null != connectorsAndStreams);
        assertTrue(1 == connectorsAndStreams.size());
        
        verify(mConnectorFactory).getConsumerConfig();
        verify(mConnectorFactory).getConsumerConnector();
        verify(mConsumerConnector).createMessageStreams(anyMap(), anyObject(), anyObject());
    }
    
    /**
     * 
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testRetry() throws Exception{
        when(mConnectorFactory.getConsumerConfig()).thenReturn(mConsumerConfig);
        when(mConnectorFactory.getConsumerConnector()).thenThrow(Exception.class).thenReturn(mConsumerConnector); // throws exception the first time
        when(mConsumerConnector.createMessageStreams(anyMap(), anyObject(), anyObject()))
            .thenReturn(ImmutableMap.of("topic", ImmutableList.of()));
        
        setStatic(KafkaSubscriber.class, "mRetryHesitationMs", 1100);
        
        KafkaSubscriber subscriber = new KafkaSubscriber(1, "topic", null, new TestConsumer(), mConnectorFactory, true, true);
        
        List<ConnectorAndStreams> connectorsAndStreams = 
                (List<ConnectorAndStreams>)ReflectionTestUtils.getField(subscriber, "mConnectorsAndStreams");
        
        TimeUnit.SECONDS.sleep(1);  // Thread creation hesitation
        
        assertTrue(null != connectorsAndStreams);
        assertTrue(0 == connectorsAndStreams.size());
        
        TimeUnit.SECONDS.sleep(2);  // Retry hesitation
        
        assertTrue(1 == connectorsAndStreams.size());
        
        verify(mConnectorFactory).getConsumerConfig();
        verify(mConnectorFactory, times(2)).getConsumerConnector();
        verify(mConsumerConnector).createMessageStreams(anyMap(), anyObject(), anyObject());
    }
    
    /**
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCleanup() throws Exception{
        when(mConnectorFactory.getConsumerConfig()).thenReturn(mConsumerConfig);
        when(mConnectorFactory.getConsumerConnector()).thenReturn(mConsumerConnector);
        when(mConsumerConnector.createMessageStreams(anyMap(), anyObject(), anyObject()))
            .thenReturn(ImmutableMap.of("topic", ImmutableList.of()));
        
        setStatic(KafkaSubscriber.class, "mRetryHesitationMs", 1100);
        
        KafkaSubscriber subscriber = new KafkaSubscriber(1, "topic", null, new TestConsumer(), mConnectorFactory, true, false);
        
        subscriber.cleanup();
        
        verify(mConnectorFactory).getConsumerConfig();
        verify(mConnectorFactory).getConsumerConnector();
        verify(mConsumerConnector).createMessageStreams(anyMap(), anyObject(), anyObject());
    }
    
    /**
     * 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    @Test
    public void consumerCreationFailureCleanup() throws Exception{
        when(mConnectorFactory.getConsumerConfig()).thenReturn(mConsumerConfig);
        when(mConnectorFactory.getConsumerConnector()).thenThrow(Exception.class);
        
        setStatic(KafkaSubscriber.class, "mRetryHesitationMs", 1100);
        
        KafkaSubscriber subscriber = new KafkaSubscriber(1, "topic", null, new TestConsumer(), mConnectorFactory, true, true);
        
        TimeUnit.SECONDS.sleep(5);
        
        subscriber.cleanup();
        
        verify(mConnectorFactory).getConsumerConfig();
        verify(mConnectorFactory, atLeastOnce()).getConsumerConnector();
    }
    
    /**
     * 
     * @param clazz
     * @param fieldName
     * @param value
     * @throws Exception
     */
    private void setStatic(Class<?> clazz, String fieldName, Object value) throws Exception{
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }
    
    /**
     * 
     */
    private static class TestConsumer{
        /**
         * 
         * @param connector
         * @param stream
         * @param mm
         */
        @SuppressWarnings("unused")
        public void consume(ConsumerConnector connector, KafkaStream<String, String> stream, 
                MessageAndMetadata<String, String> mm){}
    }
}
