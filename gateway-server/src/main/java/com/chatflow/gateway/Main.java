package com.chatflow.gateway;

import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Entry point for the ChatFlow gateway server.
 *
 * Configuration via environment variables (with defaults for local dev):
 *   RABBITMQ_HOST     - RabbitMQ host (default: localhost)
 *   RABBITMQ_PORT     - RabbitMQ port (default: 5672)
 *   RABBITMQ_USER     - RabbitMQ username (default: guest)
 *   RABBITMQ_PASS     - RabbitMQ password (default: guest)
 *   CHANNEL_POOL_SIZE - Number of pooled channels (default: 50)
 *   WS_PORT           - WebSocket server port (default: 8080)
 *   HTTP_PORT         - Health check HTTP port (default: 8081)
 *   SERVER_ID         - Unique server identifier (default: auto-generated)
 */
public class Main {

    public static void main(String[] args) throws Exception {

        // Read configuration from environment (or use defaults)
        String rabbitHost = getEnv("RABBITMQ_HOST", "localhost");
        int rabbitPort = Integer.parseInt(getEnv("RABBITMQ_PORT", "5672"));
        String rabbitUser = getEnv("RABBITMQ_USER", "guest");
        String rabbitPass = getEnv("RABBITMQ_PASS", "guest");
        int channelPoolSize = Integer.parseInt(getEnv("CHANNEL_POOL_SIZE", "50"));
        int wsPort = Integer.parseInt(getEnv("WS_PORT", "8080"));
        int httpPort = Integer.parseInt(getEnv("HTTP_PORT", "8081"));
        String serverId = getEnv("SERVER_ID", "server-" + UUID.randomUUID().toString().substring(0, 8));

        System.out.println("=== ChatFlow Gateway Server ===");
        System.out.println("RabbitMQ: " + rabbitHost + ":" + rabbitPort);
        System.out.println("Channel Pool Size: " + channelPoolSize);
        System.out.println("Server ID: " + serverId);

        // Initialize RabbitMQ publisher
        RabbitMQPublisher publisher = new RabbitMQPublisher(
                rabbitHost, rabbitPort, rabbitUser, rabbitPass, channelPoolSize);

        // Start WebSocket server
        ChatServer wsServer = new ChatServer(wsPort, publisher, serverId);
        wsServer.start();
        System.out.println("WebSocket server started on port " + wsPort);

        // Start HTTP health endpoint
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        
        httpServer.createContext("/health", exchange -> {
            String response = String.format(
                "{\"status\":\"healthy\",\"service\":\"ChatFlow Gateway Server\","
                + "\"serverId\":\"%s\","
                + "\"metrics\":{"
                    + "\"messagesReceived\":%d,"
                    + "\"messagesPublished\":%d,"
                    + "\"errors\":%d,"
                    + "\"queuePublished\":%d,"
                    + "\"queueFailures\":%d,"
                    + "\"circuitBreaker\":\"%s\","
                    + "\"availableChannels\":%d"
                + "}}",
                serverId,
                wsServer.getTotalReceived(),
                wsServer.getTotalPublished(),
                wsServer.getTotalErrors(),
                publisher.getMessagesPublished(),
                publisher.getPublishFailures(),
                publisher.getCircuitBreakerState(),
                publisher.getAvailableChannels()
            );
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        });

        httpServer.start();
        System.out.println("HTTP health endpoint started on port " + httpPort);

        // Shutdown hook for graceful cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            try { wsServer.stop(); } catch (Exception ignored) {}
            publisher.shutdown();
            httpServer.stop(0);
            System.out.println("Shutdown complete.");
        }));
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
