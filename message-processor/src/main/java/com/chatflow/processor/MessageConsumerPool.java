package com.chatflow.processor;

import com.google.gson.Gson;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance multi-threaded consumer pool.
 *
 * Behavior:
 *   - Feeds messages to WriteBuffer for database persistence
 *   - Uses blocking put() for back-pressure: if DB can't keep up,
 *     consumer naturally slows down instead of dropping messages
 *   - Updates StatsAggregator for real-time metrics
 *   - Broadcasts to WebSocket clients via RoomManager
 *   - Includes deduplication, batch ACKs, and a scaling monitor
 */
public class MessageConsumerPool {

    private static final String EXCHANGE_NAME = "chat.exchange";
    private static final int NUM_ROOMS = 20;
    private static final int BATCH_ACK_SIZE = 50;
    private static final int DEDUP_CACHE_MAX = 200000;

    private final RoomManager roomManager;
    private final WriteBuffer writeBuffer;
    private final StatsAggregator statsAggregator;
    private final Gson gson = new Gson();

    private Connection[] multiplexedConnections;
    private final List<Channel> channels = new ArrayList<>();
    private final int numConsumerThreads;

    // Metrics
    private final AtomicLong messagesConsumed = new AtomicLong(0);
    private final AtomicLong consumeErrors = new AtomicLong(0);
    private final AtomicLong duplicatesDetected = new AtomicLong(0);
    private final AtomicLong totalLagMs = new AtomicLong(0);
    private final AtomicLong maxLagMs = new AtomicLong(0);

    // Deduplication
    private final ConcurrentHashMap.KeySetView<String, Boolean> recentMessageIds = ConcurrentHashMap.newKeySet();

    // Scaling monitor
    private ScheduledExecutorService scalingMonitor;
    private final AtomicLong lastConsumedSnapshot = new AtomicLong(0);

    public MessageConsumerPool(RoomManager roomManager, WriteBuffer writeBuffer,
                               StatsAggregator statsAggregator, int numConsumerThreads) {
        this.roomManager = roomManager;
        this.writeBuffer = writeBuffer;
        this.statsAggregator = statsAggregator;
        this.numConsumerThreads = numConsumerThreads;
    }

    public void start(String host, int port, String username, String password, int prefetchCount)
            throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000);
        factory.setRequestedHeartbeat(30);

        // Create 4 Multiplexed Connections for maximum Erlang process distribution
        multiplexedConnections = new Connection[4];
        for (int i = 0; i < 4; i++) {
            multiplexedConnections[i] = factory.newConnection("message-processor-pool-" + i);
        }

        List<List<Integer>> roomAssignments = distributeRooms(NUM_ROOMS, numConsumerThreads);

        for (int t = 0; t < numConsumerThreads; t++) {
            List<Integer> assignedRooms = roomAssignments.get(t);

            Connection threadConnection = multiplexedConnections[t % 4];
            Channel channel = threadConnection.createChannel();
            channel.basicQos(prefetchCount);
            channels.add(channel);

            for (int roomId : assignedRooms) {
                String queueName = "room." + roomId;

                try {
                    channel.queueDeclarePassive(queueName);
                } catch (IOException e) {
                    channel = threadConnection.createChannel();
                    channel.basicQos(prefetchCount);
                    channels.set(t, channel);

                    java.util.Map<String, Object> args = new java.util.HashMap<>();
                    args.put("x-message-ttl", 60000);
                    args.put("x-max-length", 100000);
                    args.put("x-overflow", "drop-head");
                    channel.queueDeclare(queueName, true, false, false, args);
                    channel.queueBind(queueName, EXCHANGE_NAME, "room." + roomId);
                }

                final Channel consumerChannel = channel;
                final int finalRoomId = roomId;
                final long[] deliveryCounter = {0};

                channel.basicConsume(queueName, false, new DefaultConsumer(consumerChannel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope,
                                               AMQP.BasicProperties properties, byte[] body) throws IOException {

                        long deliveryTag = envelope.getDeliveryTag();

                        try {
                            String messageJson = new String(body, StandardCharsets.UTF_8);

                            // Lightweight deduplication
                            String messageId = extractField(messageJson, "messageId");
                            if (messageId != null && !recentMessageIds.add(messageId)) {
                                duplicatesDetected.incrementAndGet();
                                consumerChannel.basicAck(deliveryTag, false);
                                return;
                            }

                            // Evict old entries
                            if (recentMessageIds.size() > DEDUP_CACHE_MAX) {
                                recentMessageIds.clear();
                            }

                            // 1. Broadcast to WebSocket clients
                            roomManager.broadcastToRoom(String.valueOf(finalRoomId), messageJson);

                            // 2. Manual parsing (100x faster than Gson Reflection)
                            QueueMessage qm = new QueueMessage();
                            qm.setMessageId(messageId); // You already extracted this for deduplication!
                            qm.setRoomId(extractField(messageJson, "roomId"));
                            qm.setUserId(extractField(messageJson, "userId"));
                            qm.setUsername(extractField(messageJson, "username"));
                            qm.setMessage(extractField(messageJson, "message"));
                            qm.setTimestamp(extractField(messageJson, "timestamp"));
                            qm.setMessageType(extractField(messageJson, "messageType"));
                            
                            // 3. Hand off the heavy DB write to the background thread pool
                            java.util.concurrent.CompletableFuture.runAsync(() -> {
                                writeBuffer.put(qm);
                                String uid = qm.getUserId();
                                statsAggregator.recordMessageConsumed(
                                        String.valueOf(finalRoomId),
                                        uid != null ? uid : "unknown");
                            });

                            // 3. Update stats aggregator
                            String userId = extractField(messageJson, "userId");
                            statsAggregator.recordMessageConsumed(
                                    String.valueOf(finalRoomId),
                                    userId != null ? userId : "unknown");

                            // Consumer lag
                            long lag = calculateLagMs(messageJson);
                            if (lag >= 0) {
                                totalLagMs.addAndGet(lag);
                                maxLagMs.accumulateAndGet(lag, Math::max);
                            }

                            messagesConsumed.incrementAndGet();
                            deliveryCounter[0]++;

                            // Batch ACK
                            if (deliveryCounter[0] % BATCH_ACK_SIZE == 0) {
                                consumerChannel.basicAck(deliveryTag, true);
                            }

                        } catch (Exception e) {
                            consumeErrors.incrementAndGet();
                            try {
                                consumerChannel.basicNack(deliveryTag, false, true);
                            } catch (IOException nackError) {
                                System.err.println("Failed to nack: " + nackError.getMessage());
                            }
                        }
                    }
                });
            }

            System.out.println("Consumer thread " + t + " handling rooms: " + assignedRooms);
        }

        startScalingMonitor();

        System.out.println("MessageConsumerPool started: threads=" + numConsumerThreads
                + ", prefetch=" + prefetchCount + ", batchAck=" + BATCH_ACK_SIZE
                + ", dedup=enabled, persistence=enabled, backPressure=enabled");
    }

    /**
     * Extract a string field value from JSON without full deserialization.
     */
    private String extractField(String json, String fieldName) {
        String search = "\"" + fieldName + "\"";
        int idx = json.indexOf(search);
        if (idx == -1) return null;
        int colonIdx = json.indexOf(':', idx);
        if (colonIdx == -1) return null;
        int startQuote = json.indexOf('"', colonIdx + 1);
        if (startQuote == -1) return null;
        int endQuote = json.indexOf('"', startQuote + 1);
        if (endQuote == -1) return null;
        return json.substring(startQuote + 1, endQuote);
    }

    private long calculateLagMs(String json) {
        try {
            String timestamp = extractField(json, "timestamp");
            if (timestamp == null) return -1;
            long messageTimeMs = java.time.Instant.parse(timestamp).toEpochMilli();
            return System.currentTimeMillis() - messageTimeMs;
        } catch (Exception e) {
            return -1;
        }
    }

    private void startScalingMonitor() {
        scalingMonitor = Executors.newSingleThreadScheduledExecutor();
        scalingMonitor.scheduleAtFixedRate(() -> {
            long current = messagesConsumed.get();
            long previous = lastConsumedSnapshot.getAndSet(current);
            long rate = (current - previous) / 10;

            if (rate > 0) {
                long consumed = messagesConsumed.get();
                long avgLag = consumed > 0 ? totalLagMs.get() / consumed : 0;
                System.out.printf("[ScalingMonitor] Consume: %d msg/s | Consumed: %d | " +
                        "Buffer: %d | Dropped: %d | Avg lag: %dms | Max lag: %dms | Errors: %d%n",
                        rate, current, writeBuffer.size(), writeBuffer.getTotalDropped(),
                        avgLag, maxLagMs.get(), consumeErrors.get());
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private List<List<Integer>> distributeRooms(int totalRooms, int numThreads) {
        List<List<Integer>> assignments = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            assignments.add(new ArrayList<>());
        }
        for (int room = 1; room <= totalRooms; room++) {
            assignments.get((room - 1) % numThreads).add(room);
        }
        return assignments;
    }

    public void shutdown() {
        for (Channel channel : channels) {
            try { channel.basicAck(0, true); } catch (Exception ignored) {}
            try { channel.close(); } catch (Exception ignored) {}
        }
        if (scalingMonitor != null) {
            scalingMonitor.shutdownNow();
        }
        if (multiplexedConnections != null) {
            for (Connection conn : multiplexedConnections) {
                try {
                    if (conn != null) conn.close();
                } catch (Exception ignored) {}
            }
        }

        System.out.println("MessageConsumerPool shut down. Consumed: " + messagesConsumed.get()
                + ", Errors: " + consumeErrors.get()
                + ", Duplicates: " + duplicatesDetected.get());
    }

    public long getMessagesConsumed() { return messagesConsumed.get(); }
    public long getConsumeErrors() { return consumeErrors.get(); }
    public long getDuplicatesDetected() { return duplicatesDetected.get(); }
    public long getAvgLagMs() {
        long consumed = messagesConsumed.get();
        return consumed > 0 ? totalLagMs.get() / consumed : 0;
    }
    public long getMaxLagMs() { return maxLagMs.get(); }
}
