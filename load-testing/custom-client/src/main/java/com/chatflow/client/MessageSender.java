package com.chatflow.client;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import com.google.gson.Gson;
import java.net.URI;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessageSender implements Runnable {

    private final BlockingQueue<ChatMessage> queue;
    private final String serverBaseUri;
    private final int messagesToSend;
    private final Gson gson = new Gson();
    private final Queue<MessageMetric> metrics;

    // Connection pool: roomId -> WebSocketClient
    private final Map<Integer, WebSocketClient> connections = new HashMap<>();
    
    // Track pending messages per room (FIFO order)
    private final Map<Integer, Queue<PendingMessage>> pendingPerRoom = new HashMap<>();

    private static final int MAX_RETRIES = 5;

    // Helper class to track pending message info
    private static class PendingMessage {
        final long sendTimestamp;
        final String messageType;
        final int roomId;

        PendingMessage(long sendTimestamp, String messageType, int roomId) {
            this.sendTimestamp = sendTimestamp;
            this.messageType = messageType;
            this.roomId = roomId;
        }
    }

    public MessageSender(BlockingQueue<ChatMessage> queue, String serverBaseUri,
                         int messagesToSend, Queue<MessageMetric> metrics) {
        this.queue = queue;
        this.serverBaseUri = serverBaseUri;
        this.messagesToSend = messagesToSend;
        this.metrics = metrics;
    }

    @Override
    public void run() {
        try {
            // Send all messages asynchronously
            for (int i = 0; i < messagesToSend; i++) {
                ChatMessage message = queue.take();
                int roomId = message.getRoomId();

                boolean sent = false;
                int retries = 0;

                while (!sent && retries < MAX_RETRIES) {
                    try {
                        WebSocketClient client = getOrCreateConnection(roomId);
                        
                        // Record send time and add to pending queue
                        long sendTime = System.currentTimeMillis();
                        PendingMessage pending = new PendingMessage(
                            sendTime, 
                            message.getMessageType(), 
                            roomId
                        );
                        pendingPerRoom.get(roomId).add(pending);
                        
                        // Send without waiting for response
                        client.send(gson.toJson(message));
                        sent = true;

                    } catch (Exception e) {
                        retries++;
                        WebSocketClient broken = connections.remove(roomId);
                        if (broken != null) {
                            try { broken.close(); } catch (Exception ignored) {}
                        }
                        if (retries < MAX_RETRIES) {
                            long backoffMs = (long) (100 * Math.pow(2, retries - 1));
                            Thread.sleep(backoffMs);
                        }
                    }
                }

                // If failed after all retries, record as failed
                if (!sent) {
                    long now = System.currentTimeMillis();
                    metrics.add(new MessageMetric(now, now, message.getMessageType(), "failed", roomId));
                }
            }
            
            // Wait for all pending responses
            waitForPendingResponses();
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Close all connections
            for (WebSocketClient client : connections.values()) {
                try { client.close(); } catch (Exception ignored) {}
            }
            connections.clear();
        }
    }
    
    private void waitForPendingResponses() {
        // Wait up to 30 seconds for all pending responses
        long startWait = System.currentTimeMillis();
        while (System.currentTimeMillis() - startWait < 600000) {
            boolean allDone = true;
            for (Queue<PendingMessage> pending : pendingPerRoom.values()) {
                if (!pending.isEmpty()) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) break;
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private WebSocketClient getOrCreateConnection(int roomId) throws Exception {
        WebSocketClient client = connections.get(roomId);
        if (client != null && client.isOpen()) {
            return client;
        }

        if (client != null) {
            connections.remove(roomId);
            try { client.close(); } catch (Exception ignored) {}
        }

        String uri = serverBaseUri + "room" + roomId;
        
        // Create pending queue for this room
        final Queue<PendingMessage> pendingQueue = new ConcurrentLinkedQueue<>();
        pendingPerRoom.put(roomId, pendingQueue);
        
        WebSocketClient newClient = new WebSocketClient(new URI(uri)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                // Connection established
            }

            @Override
            public void onMessage(String message) {
                // Async response handling - match to pending message (FIFO)
                long receiveTime = System.currentTimeMillis();
                PendingMessage pending = pendingQueue.poll();
                
                if (pending != null) {
                    String status = message.contains("\"status\":\"success\"") ? "success" : "error";
                    MessageMetric metric = new MessageMetric(
                        pending.sendTimestamp,
                        receiveTime,
                        pending.messageType,
                        status,
                        pending.roomId
                    );
                    metrics.add(metric);
                }
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                // Connection closed - mark any remaining pending as failed
                PendingMessage pending;
                while ((pending = pendingQueue.poll()) != null) {
                    long now = System.currentTimeMillis();
                    metrics.add(new MessageMetric(
                        pending.sendTimestamp, 
                        now, 
                        pending.messageType, 
                        "connection_closed", 
                        pending.roomId
                    ));
                }
            }

            @Override
            public void onError(Exception ex) {
                // Error occurred
            }
        };

        newClient.setTcpNoDelay(true);
        newClient.connectBlocking();
        connections.put(roomId, newClient);
        return newClient;
    }
}