package com.chatflow.client;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.Queue;

/**
 * ChatFlow load-test client.
 *
 * Usage:
 *   java -jar client.jar <server-uri> <metrics-api-url> [total-messages]
 */
public class ClientMain {

    private static final int DEFAULT_TOTAL_MESSAGES = 500_000;
    private static final int WARMUP_THREADS = 32;
    private static final int WARMUP_MESSAGES_PER_THREAD = 1000;
    private static final int MAIN_PHASE_THREADS = 32;
    private static final String DEFAULT_SERVER_URI = "ws://localhost:8080/chat/";
    private static final String DEFAULT_METRICS_URL = "http://localhost:9091";
    private static final int QUEUE_CAPACITY = 10000;

    public static void main(String[] args) throws Exception {

        String serverBaseUri = args.length > 0 ? args[0] : DEFAULT_SERVER_URI;
        String metricsApiUrl = args.length > 1 ? args[1] : DEFAULT_METRICS_URL;
        int totalMessages = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_TOTAL_MESSAGES;

        System.out.println("=== ChatFlow Load Test Client ===");
        System.out.println("Server URI: " + serverBaseUri);
        System.out.println("Metrics API: " + metricsApiUrl);
        System.out.println("Total Messages: " + totalMessages);

        BlockingQueue<ChatMessage> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        Queue<MessageMetric> metrics = new ConcurrentLinkedQueue<>();

        Thread generatorThread = new Thread(new MessageGenerator(queue, totalMessages));
        generatorThread.start();

        long startTime = System.currentTimeMillis();

        // ============ WARMUP PHASE ============
        int warmupBudget = WARMUP_THREADS * WARMUP_MESSAGES_PER_THREAD;
        int actualWarmupThreads, actualWarmupPerThread;

        if (totalMessages <= warmupBudget) {
            actualWarmupPerThread = WARMUP_MESSAGES_PER_THREAD;
            actualWarmupThreads = Math.max(1, totalMessages / (2 * actualWarmupPerThread));
            if (actualWarmupThreads == 0) {
                actualWarmupThreads = 1;
                actualWarmupPerThread = Math.min(totalMessages / 2, WARMUP_MESSAGES_PER_THREAD);
            }
        } else {
            actualWarmupThreads = WARMUP_THREADS;
            actualWarmupPerThread = WARMUP_MESSAGES_PER_THREAD;
        }

        int totalWarmupMessages = actualWarmupThreads * actualWarmupPerThread;
        System.out.println("=== WARMUP PHASE: Starting " + actualWarmupThreads
                + " threads x " + actualWarmupPerThread + " msgs (" + totalWarmupMessages + " total) ===");

        ExecutorService warmupExecutor = Executors.newFixedThreadPool(actualWarmupThreads);
        for (int i = 0; i < actualWarmupThreads; i++) {
            warmupExecutor.submit(new MessageSender(queue, serverBaseUri, actualWarmupPerThread, metrics));
        }
        warmupExecutor.shutdown();
        warmupExecutor.awaitTermination(30, TimeUnit.MINUTES);

        long warmupEndTime = System.currentTimeMillis();
        System.out.println("Warmup completed in " + (warmupEndTime - startTime) + " ms");

        // ============ MAIN PHASE ============
        int mainMessages = totalMessages - totalWarmupMessages;
        System.out.println("=== MAIN PHASE: Starting " + MAIN_PHASE_THREADS
                + " threads, " + mainMessages + " messages ===");

        int messagesPerThread = mainMessages / MAIN_PHASE_THREADS;
        int extraMessages = mainMessages % MAIN_PHASE_THREADS;

        ExecutorService mainPhaseExecutor = Executors.newFixedThreadPool(MAIN_PHASE_THREADS);
        for (int i = 0; i < MAIN_PHASE_THREADS; i++) {
            int toSend = messagesPerThread + (i < extraMessages ? 1 : 0);
            mainPhaseExecutor.submit(new MessageSender(queue, serverBaseUri, toSend, metrics));
        }
        mainPhaseExecutor.shutdown();
        mainPhaseExecutor.awaitTermination(30, TimeUnit.MINUTES);

        long endTime = System.currentTimeMillis();

        // ============ WRITE CSV ============
        writeMetricsToCsv(metrics, "metrics.csv");
        writeThroughputCsv(metrics, "throughput.csv");

        // ============ PRINT CLIENT METRICS ============
        double totalSeconds = (endTime - startTime) / 1000.0;
        double throughput = totalMessages / totalSeconds;

        System.out.println("\n=== CLIENT RESULTS ===");
        System.out.println("Total messages: " + metrics.size());
        System.out.println("Total time: " + totalSeconds + " seconds");
        System.out.println("Throughput: " + throughput + " messages/second");

        printStatistics(metrics);
        printThroughputPerRoom(metrics, totalSeconds);
        printMessageTypeDistribution(metrics);

        // ============ WAIT FOR DB WRITES TO DRAIN ============
        System.out.println("\n=== Waiting for database writes to complete... ===");
        waitForBufferDrain(metricsApiUrl, totalMessages);

        // ============ CALL METRICS API ============
        System.out.println("\n=== Calling Metrics API... ===");
        MetricsAPIClient metricsClient = new MetricsAPIClient(metricsApiUrl);
        metricsClient.fetchAndLogAllResults();
    }

    /**
     * Wait for the consumer's write buffer to drain.
     *
     * Strategy for EC2 t3.micro (1 vCPU):
     *   On a micro instance, the single CPU is saturated during DB writes.
     *   The HTTP server can't respond while writes are in progress.
     *   Instead of polling (which times out), we use a hybrid approach:
     *   1. Calculate estimated drain time based on message count
     *   2. Wait that time with periodic status prints
     *   3. Then try polling with generous timeouts to confirm drain
     *   4. Add cool-down period for CPU to settle before fetching metrics
     */
    private static void waitForBufferDrain(String metricsApiUrl, int totalMessages) {
        // Estimate drain time: ~3000-5000 msg/s on EC2, ~10000 msg/s local
        // Use conservative estimate for EC2
        int estimatedDrainSeconds = Math.max(30, totalMessages / 2000);
        // Cap at 10 minutes
        estimatedDrainSeconds = Math.min(estimatedDrainSeconds, 600);

        System.out.println("  Estimated drain time: ~" + estimatedDrainSeconds + "s for " + totalMessages + " messages");
        System.out.println("  Waiting for writes to complete (checking every 30s)...");

        long startWait = System.currentTimeMillis();
        int waited = 0;

        // Phase 1: Wait with periodic poll attempts
        while (waited < estimatedDrainSeconds + 120) { // extra 2 min buffer
            try {
                Thread.sleep(30000); // Wait 30s between checks
                waited += 30;

                long elapsed = (System.currentTimeMillis() - startWait) / 1000;

                // Try a quick poll — if it succeeds, great; if it times out, keep waiting
                try {
                    URL url = new URL(metricsApiUrl + "/api/health");
                    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                    conn.setRequestMethod("GET");
                    conn.setConnectTimeout(15000);
                    conn.setReadTimeout(30000);

                    if (conn.getResponseCode() == 200) {
                        // Server is responsive! Now check buffer depth
                        conn.disconnect();

                        URL metricsUrl = new URL(metricsApiUrl + "/api/metrics");
                        HttpURLConnection mConn = (HttpURLConnection) metricsUrl.openConnection();
                        mConn.setRequestMethod("GET");
                        mConn.setConnectTimeout(15000);
                        mConn.setReadTimeout(60000);

                        if (mConn.getResponseCode() == 200) {
                            BufferedReader reader = new BufferedReader(new InputStreamReader(mConn.getInputStream()));
                            StringBuilder response = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) response.append(line);
                            reader.close();

                            String json = response.toString();
                            int bufferDepth = extractJsonInt(json, "currentDepth");
                            long totalEnqueued = extractJsonLong(json, "totalEnqueued");
                            long totalDropped = extractJsonLong(json, "totalDropped");

                            System.out.printf("  [%ds] Buffer: %d | Enqueued: %d | Dropped: %d%n",
                                    elapsed, bufferDepth, totalEnqueued, totalDropped);

                            if (bufferDepth == 0 && totalEnqueued > 0) {
                                System.out.println("  Buffer drained! All messages persisted. (waited " + elapsed + "s)");
                                // Cool-down: let CPU settle before metrics queries
                                System.out.println("  Cooling down 15s before fetching metrics...");
                                Thread.sleep(15000);
                                return;
                            }
                        }
                        mConn.disconnect();
                    } else {
                        conn.disconnect();
                        System.out.printf("  [%ds] Consumer busy (HTTP %d), still draining...%n",
                                elapsed, conn.getResponseCode());
                    }
                } catch (Exception pollErr) {
                    // Server is busy with DB writes — this is expected on t3.micro
                    System.out.printf("  [%ds] Consumer CPU busy (writes in progress), waiting...%n", elapsed);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Phase 2: If we get here, drain likely completed but we couldn't confirm
        System.out.println("  Wait period completed. Cooling down 30s before fetching metrics...");
        try { Thread.sleep(30000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private static int extractJsonInt(String json, String key) {
        String search = "\"" + key + "\":";
        int idx = json.indexOf(search);
        if (idx == -1) return -1;
        int start = idx + search.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) end++;
        try { return Integer.parseInt(json.substring(start, end)); }
        catch (NumberFormatException e) { return -1; }
    }

    private static long extractJsonLong(String json, String key) {
        String search = "\"" + key + "\":";
        int idx = json.indexOf(search);
        if (idx == -1) return -1;
        int start = idx + search.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) end++;
        try { return Long.parseLong(json.substring(start, end)); }
        catch (NumberFormatException e) { return -1; }
    }

    // ============ STATISTICS METHODS ============

    private static void writeMetricsToCsv(Queue<MessageMetric> metrics, String filename) throws Exception {
        List<MessageMetric> metricsCopy = new ArrayList<>(metrics);
        PrintWriter writer = new PrintWriter(new FileWriter(filename));
        writer.println("timestamp,messageType,latencyMs,status,roomId");
        for (MessageMetric metric : metricsCopy) {
            writer.println(metric.getSendTimestamp() + ","
                         + metric.getMessageType() + ","
                         + metric.getLatencyMs() + ","
                         + metric.getStatus() + ","
                         + metric.getRoomId());
        }
        writer.close();
        System.out.println("Metrics written to " + filename);
    }

    private static void printStatistics(Queue<MessageMetric> metrics) {
        List<MessageMetric> metricsCopy = new ArrayList<>(metrics);
        if (metricsCopy.isEmpty()) { System.out.println("No metrics to analyze!"); return; }

        long minSend = Long.MAX_VALUE;
        long maxReceive = Long.MIN_VALUE;
        List<Long> roundTrips = new ArrayList<>();
        int confirmed = 0;
        int failed = 0;

        for (MessageMetric m : metricsCopy) {
            if (m.getSendTimestamp() < minSend) minSend = m.getSendTimestamp();
            if (m.getReceiveTimestamp() > maxReceive) maxReceive = m.getReceiveTimestamp();
            
            if ("success".equals(m.getStatus())) {
                confirmed++;
                roundTrips.add(m.getLatencyMs());
            } else {
                failed++;
            }
        }

        Collections.sort(roundTrips);
        long p50 = roundTrips.isEmpty() ? 0 : roundTrips.get((int)(confirmed * 0.50));
        long p99 = roundTrips.isEmpty() ? 0 : roundTrips.get((int)(confirmed * 0.99));
        long maxRt = roundTrips.isEmpty() ? 0 : roundTrips.get(confirmed - 1);
        
        // Exact Round-trip Math: Total time from the very first message leaving the client 
        // to the very last broadcast being received back.
        double roundTripSeconds = (maxReceive - minSend) / 1000.0;
        long roundTripThroughput = roundTripSeconds > 0 ? (long)(confirmed / roundTripSeconds) : 0;

        System.out.println("\n========== Load Test Results ==========");
        System.out.printf("Messages sent         : %,d%n", metricsCopy.size());
        System.out.printf("Failed messages       : %,d%n", failed);
        System.out.printf("Total runtime         : %.2f s%n", roundTripSeconds);
        System.out.println("---------------------------------------");
        System.out.printf("Round-trip confirmed  : %,d  (%.1f%% of sent)%n", confirmed, (confirmed * 100.0) / metricsCopy.size());
        System.out.printf("Round-trip throughput : %,d msg/sec%n", roundTripThroughput);
        System.out.printf("p50 round-trip        : %d ms%n", p50);
        System.out.printf("p99 round-trip        : %d ms%n", p99);
        System.out.printf("Max round-trip        : %d ms%n", maxRt);
        System.out.println("---------------------------------------");
        System.out.println("=======================================\n");
    }

    private static void printThroughputPerRoom(Queue<MessageMetric> metrics, double totalSeconds) {
        List<MessageMetric> metricsCopy = new ArrayList<>(metrics);
        if (metricsCopy.isEmpty()) return;

        Map<Integer, Integer> roomCounts = new TreeMap<>();
        for (MessageMetric m : metricsCopy)
            roomCounts.put(m.getRoomId(), roomCounts.getOrDefault(m.getRoomId(), 0) + 1);

        System.out.println("\n=== THROUGHPUT PER ROOM ===");
        for (Map.Entry<Integer, Integer> entry : roomCounts.entrySet())
            System.out.printf("Room %d: %d messages, %.2f msg/sec%n",
                    entry.getKey(), entry.getValue(), entry.getValue() / totalSeconds);
    }

    private static void printMessageTypeDistribution(Queue<MessageMetric> metrics) {
        List<MessageMetric> metricsCopy = new ArrayList<>(metrics);
        if (metricsCopy.isEmpty()) return;

        Map<String, Integer> typeCounts = new TreeMap<>();
        for (MessageMetric m : metricsCopy)
            typeCounts.put(m.getMessageType(), typeCounts.getOrDefault(m.getMessageType(), 0) + 1);

        int total = metricsCopy.size();
        System.out.println("\n=== MESSAGE TYPE DISTRIBUTION ===");
        for (Map.Entry<String, Integer> entry : typeCounts.entrySet())
            System.out.printf("%s: %d (%.1f%%)%n", entry.getKey(), entry.getValue(),
                    (entry.getValue() * 100.0) / total);
    }

    private static void writeThroughputCsv(Queue<MessageMetric> metrics, String filename) throws Exception {
        List<MessageMetric> metricsCopy = new ArrayList<>(metrics);
        if (metricsCopy.isEmpty()) return;

        long startTime = Long.MAX_VALUE, endTime = Long.MIN_VALUE;
        for (MessageMetric m : metricsCopy) {
            if (m.getSendTimestamp() < startTime) startTime = m.getSendTimestamp();
            if (m.getSendTimestamp() > endTime) endTime = m.getSendTimestamp();
        }

        Map<Integer, Integer> buckets = new TreeMap<>();
        for (MessageMetric m : metricsCopy) {
            int bucket = (int) ((m.getSendTimestamp() - startTime) / 10000);
            buckets.put(bucket, buckets.getOrDefault(bucket, 0) + 1);
        }

        int totalBuckets = (int) ((endTime - startTime) / 10000) + 1;
        PrintWriter writer = new PrintWriter(new FileWriter(filename));
        writer.println("time_seconds,messages_in_bucket,throughput_per_second");
        for (int i = 0; i < totalBuckets; i++) {
            int count = buckets.getOrDefault(i, 0);
            writer.println((i * 10) + "," + count + "," + (count / 10.0));
        }
        writer.close();
        System.out.println("Throughput data written to " + filename);
    }
}
