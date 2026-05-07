package com.chatflow.gateway;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages RabbitMQ publishing with channel pooling and circuit breaker.
 * 
 * Topology:
 *   Exchange: chat.exchange (topic)
 *   Routing keys: room.{roomId}
 *   Queues: room.1 through room.20, each bound to its routing key
 */
public class RabbitMQPublisher {

    private static final String EXCHANGE_NAME = "chat.exchange";
    private static final int NUM_ROOMS = 20;
    private static final int MESSAGE_TTL_MS = 60000; // 60 seconds TTL
    private static final int MAX_QUEUE_LENGTH = 100000; // 100K messages per queue

    private final ChannelPool channelPool;
    private final CircuitBreaker circuitBreaker;

    // Metrics
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong publishFailures = new AtomicLong(0);

    public RabbitMQPublisher(String host, int port, String username, String password, int poolSize)
            throws IOException, TimeoutException {
        
        this.channelPool = new ChannelPool(host, port, username, password, poolSize);
        this.circuitBreaker = new CircuitBreaker(5, 10000); // 5 failures → open for 10s

        // Declare exchange and queues on startup
        declareTopology();
    }

    /**
     * Declare the topic exchange and per-room queues with bindings.
     */
    private void declareTopology() throws IOException {
        Channel channel = null;
        try {
            channel = channelPool.borrowChannel();
            if (channel == null) {
                throw new IOException("Could not borrow channel to declare topology");
            }

            // Declare topic exchange (durable so it survives broker restart)
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);

            // Declare one queue per room with TTL and max-length
            for (int i = 1; i <= NUM_ROOMS; i++) {
                String queueName = "room." + i;
                String routingKey = "room." + i;

                Map<String, Object> args = new HashMap<>();
                args.put("x-message-ttl", MESSAGE_TTL_MS);
                args.put("x-max-length", MAX_QUEUE_LENGTH);
                args.put("x-overflow", "drop-head"); // Drop oldest if queue full

                // durable=true, exclusive=false, autoDelete=false
                channel.queueDeclare(queueName, true, false, false, args);
                channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
            }

            System.out.println("RabbitMQ topology declared: exchange=" + EXCHANGE_NAME 
                    + ", queues=room.1..room." + NUM_ROOMS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while declaring topology", e);
        } finally {
            channelPool.returnChannel(channel);
        }
    }

    /**
     * Publish a message to the appropriate room queue.
     * Uses fire-and-forget publishing for maximum throughput.
     * Circuit breaker provides fault tolerance if RabbitMQ goes down.
     * 
     * @param messageJson JSON string of the QueueMessage
     * @param roomId      the room ID for routing
     * @return true if published successfully
     */
    public boolean publish(String messageJson, String roomId) {
        // Check circuit breaker first
        if (!circuitBreaker.allowRequest()) {
            publishFailures.incrementAndGet();
            return false;
        }

        Channel channel = null;
        try {
            channel = channelPool.borrowChannel();
            if (channel == null) {
                circuitBreaker.recordFailure();
                publishFailures.incrementAndGet();
                return false;
            }

            String routingKey = "room." + roomId;

            // Non-persistent for performance (deliveryMode=1), persistent (deliveryMode=2)
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .deliveryMode(1) // non-persistent for higher throughput
                    .contentType("application/json")
                    .build();

            // Fire-and-forget: publish without waiting for confirm
            channel.basicPublish(EXCHANGE_NAME, routingKey, props, messageJson.getBytes());

            circuitBreaker.recordSuccess();
            messagesPublished.incrementAndGet();
            return true;

        } catch (Exception e) {
            circuitBreaker.recordFailure();
            publishFailures.incrementAndGet();
            System.err.println("Publish failed: " + e.getMessage());
            return false;
        } finally {
            channelPool.returnChannel(channel);
        }
    }

    // Metrics getters
    public long getMessagesPublished() { return messagesPublished.get(); }
    public long getPublishFailures() { return publishFailures.get(); }
    public CircuitBreaker.State getCircuitBreakerState() { return circuitBreaker.getState(); }
    public int getAvailableChannels() { return channelPool.getAvailableCount(); }

    /**
     * Graceful shutdown.
     */
    public void shutdown() {
        channelPool.shutdown();
        System.out.println("RabbitMQPublisher shut down. Published: " + messagesPublished.get() 
                + ", Failures: " + publishFailures.get());
    }
}