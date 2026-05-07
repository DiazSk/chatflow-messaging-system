package com.chatflow.gateway;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe pool of RabbitMQ channels.
 * Channels are pre-created and borrowed/returned by publisher threads.
 */
public class ChannelPool {

    private final BlockingQueue<Channel> pool;
    private final Connection connection;
    private final int poolSize;
    private final AtomicBoolean isOpen = new AtomicBoolean(true);

    /**
     * Create a channel pool with the given size connected to the specified RabbitMQ host.
     */
    public ChannelPool(String host, int port, String username, String password, int poolSize) 
            throws IOException, TimeoutException {
        this.poolSize = poolSize;
        this.pool = new ArrayBlockingQueue<>(poolSize);

        // Create the single shared connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);

        // Connection recovery settings
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);
        factory.setRequestedHeartbeat(30);

        this.connection = factory.newConnection("gateway-server-producer");

        // Pre-create channels
        for (int i = 0; i < poolSize; i++) {
            Channel channel = connection.createChannel();
            pool.offer(channel);
        }
        System.out.println("ChannelPool initialized with " + poolSize + " channels");
    }

    /**
     * Borrow a channel from the pool. Blocks up to 5 seconds.
     * @return a Channel, or null if timeout
     */
    public Channel borrowChannel() throws InterruptedException {
        if (!isOpen.get()) {
            throw new IllegalStateException("ChannelPool is closed");
        }
        Channel channel = pool.poll(5, TimeUnit.SECONDS);
        if (channel != null && !channel.isOpen()) {
            // Channel was closed; create a replacement
            try {
                channel = connection.createChannel();
            } catch (IOException e) {
                System.err.println("Failed to create replacement channel: " + e.getMessage());
                return null;
            }
        }
        return channel;
    }

    /**
     * Return a channel back to the pool.
     */
    public void returnChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            if (!pool.offer(channel)) {
                // Pool is full (shouldn't happen), close the extra channel
                try { channel.close(); } catch (Exception ignored) {}
            }
        } else if (channel != null) {
            // Channel is dead, create a replacement
            try {
                Channel replacement = connection.createChannel();
                pool.offer(replacement);
            } catch (Exception e) {
                System.err.println("Failed to create replacement channel: " + e.getMessage());
            }
        }
    }

    /**
     * Get the number of available channels in the pool.
     */
    public int getAvailableCount() {
        return pool.size();
    }

    /**
     * Check if the underlying connection is still open.
     */
    public boolean isConnectionOpen() {
        return connection != null && connection.isOpen();
    }

    /**
     * Shut down the pool and close all channels + the connection.
     */
    public void shutdown() {
        isOpen.set(false);
        for (Channel channel : pool) {
            try { channel.close(); } catch (Exception ignored) {}
        }
        pool.clear();
        try { connection.close(); } catch (Exception ignored) {}
        System.out.println("ChannelPool shut down");
    }
}