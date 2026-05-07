package com.chatflow.gateway;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import com.google.gson.Gson;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WebSocket server that acts as a MESSAGE PRODUCER.
 * Instead of echoing messages back, it validates and publishes them to RabbitMQ.
 * The consumer application (separate process) handles message distribution.
 */
public class ChatServer extends WebSocketServer {

    private final Gson gson = new Gson();
    private final RabbitMQPublisher publisher;
    private final String serverId;

    // Metrics
    private final AtomicLong totalReceived = new AtomicLong(0);
    private final AtomicLong totalPublished = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);

    public ChatServer(int port, RabbitMQPublisher publisher, String serverId) {
        super(new InetSocketAddress(port));
        this.publisher = publisher;
        this.serverId = serverId;
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String resourceDescriptor = conn.getResourceDescriptor(); // e.g. /chat/room5
        String roomId = extractRoomId(resourceDescriptor);
        if (!isValidRoomId(roomId)) {
            conn.close(1008, "Invalid roomId. Use /chat/room{1-20}");
            System.out.println("Rejected connection: " + conn.getRemoteSocketAddress()
                    + " | invalid room path: " + resourceDescriptor);
            return;
        }
        // Store validated roomId as attachment for easy access in onMessage
        conn.setAttachment(roomId);
        System.out.println("New connection: " + conn.getRemoteSocketAddress() + " | room: " + roomId);
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        totalReceived.incrementAndGet();
        
        try {
            ChatMessage chatMessage = gson.fromJson(message, ChatMessage.class);
            String serverTimestamp = Instant.now().toString();

            // Validate the message
            String validationError = validateMessage(chatMessage);
            if (validationError != null) {
                ServerResponse response = new ServerResponse(chatMessage, serverTimestamp, "error", validationError);
                conn.send(gson.toJson(response));
                totalErrors.incrementAndGet();
                return;
            }

            // Extract room ID from the WebSocket path
            String roomId = conn.getAttachment();
            if (!isValidRoomId(roomId)) {
                ServerResponse response = new ServerResponse(chatMessage, serverTimestamp, "error",
                        "Invalid roomId. Use /chat/room{1-20}");
                conn.send(gson.toJson(response));
                totalErrors.incrementAndGet();
                return;
            }

            // Get client IP
            String clientIp = conn.getRemoteSocketAddress() != null 
                    ? conn.getRemoteSocketAddress().getAddress().getHostAddress() 
                    : "unknown";

            // Build the enriched queue message
            QueueMessage queueMessage = QueueMessage.fromChatMessage(chatMessage, roomId, serverId, clientIp);
            String queueJson = gson.toJson(queueMessage);

            // Publish to RabbitMQ
            boolean published = publisher.publish(queueJson, roomId);

            if (published) {
                // Acknowledge to the client that message was accepted
                ServerResponse response = new ServerResponse(chatMessage, serverTimestamp, "success", null);
                conn.send(gson.toJson(response));
                totalPublished.incrementAndGet();
            } else {
                // Queue unavailable - inform client
                ServerResponse response = new ServerResponse(chatMessage, serverTimestamp, "error", 
                        "Message queue unavailable, please retry");
                conn.send(gson.toJson(response));
                totalErrors.incrementAndGet();
            }

        } catch (Exception e) {
            ServerResponse response = new ServerResponse(null, Instant.now().toString(), "error",
                    "Internal server error: " + e.getMessage());
            conn.send(gson.toJson(response));
            totalErrors.incrementAndGet();
        }
    }

    /**
     * Extract room ID from the WebSocket resource descriptor.
     * Expected format: /chat/room{id} or /chat/{id}
     */
    private String extractRoomId(String resourceDescriptor) {
        if (resourceDescriptor == null) return "unknown";
        
        if (resourceDescriptor.startsWith("/chat/room")) {
            return resourceDescriptor.substring("/chat/room".length());
        } else if (resourceDescriptor.startsWith("/chat/")) {
            return resourceDescriptor.substring("/chat/".length());
        }
        return "unknown";
    }

    private boolean isValidRoomId(String roomId) {
        if (roomId == null || roomId.isEmpty()) return false;
        try {
            int parsed = Integer.parseInt(roomId);
            return parsed >= 1 && parsed <= 20;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String validateMessage(ChatMessage msg) {
        // Validate userId
        if (msg.getUserId() == null || msg.getUserId().isEmpty()) {
            return "userID is required";
        }
        try {
            int userId = Integer.parseInt(msg.getUserId());
            if (userId < 1 || userId > 100000) {
                return "userID must be between 1 and 100000";
            }
        } catch (NumberFormatException e) {
            return "userID must be a valid number";
        }

        // Validate username
        if (msg.getUsername() == null || msg.getUsername().isEmpty()) {
            return "username is required";
        }
        if (!msg.getUsername().matches("^[a-zA-Z0-9]+$")) {
            return "username must be alphanumeric (only letters and numbers)";
        }
        int usernameLength = msg.getUsername().length();
        if (usernameLength < 3 || usernameLength > 20) {
            return "username must be between 3 and 20 characters";
        }

        // Validate message
        if (msg.getMessage() == null || msg.getMessage().isEmpty()) {
            return "message is required";
        }
        if (msg.getMessage().length() > 500) {
            return "message must be 1-500 characters";
        }

        // Validate messageType
        if (msg.getMessageType() == null) {
            return "messageType is required";
        }
        String type = msg.getMessageType();
        if (!type.equals("TEXT") && !type.equals("JOIN") && !type.equals("LEAVE")) {
            return "messageType must be TEXT, JOIN, or LEAVE";
        }

        // Validate timestamp
        if (msg.getTimestamp() == null || msg.getTimestamp().isEmpty()) {
            return "timestamp is required";
        }
        try {
            Instant.parse(msg.getTimestamp());
        } catch (Exception e) {
            return "timestamp must be a valid ISO 8601 format";
        }

        return null;
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        System.out.println("Connection closed: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("WebSocket error: " + ex.getMessage());
    }

    @Override
    public void onStart() {
        System.out.println("ChatServer started on port: " + getPort() + " | serverId: " + serverId);
    }

    // Metrics getters for health endpoint
    public long getTotalReceived() { return totalReceived.get(); }
    public long getTotalPublished() { return totalPublished.get(); }
    public long getTotalErrors() { return totalErrors.get(); }
}
