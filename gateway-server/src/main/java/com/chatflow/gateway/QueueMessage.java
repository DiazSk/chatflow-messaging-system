package com.chatflow.gateway;

import java.util.UUID;

/**
 * Enriched message format published to RabbitMQ.
 * Adds messageId, roomId, serverId, and clientIp on top of the original ChatMessage fields.
 */
public class QueueMessage {
    private String messageId;
    private String roomId;
    private String userId;
    private String username;
    private String message;
    private String timestamp;
    private String messageType;
    private String serverId;
    private String clientIp;

    public QueueMessage() {}

    /**
     * Build a QueueMessage from an incoming ChatMessage + server context.
     */
    public static QueueMessage fromChatMessage(ChatMessage chatMsg, String roomId, String serverId, String clientIp) {
        QueueMessage qm = new QueueMessage();
        qm.messageId = UUID.randomUUID().toString();
        qm.roomId = roomId;
        qm.userId = chatMsg.getUserId();
        qm.username = chatMsg.getUsername();
        qm.message = chatMsg.getMessage();
        qm.timestamp = chatMsg.getTimestamp();
        qm.messageType = chatMsg.getMessageType();
        qm.serverId = serverId;
        qm.clientIp = clientIp;
        return qm;
    }

    // Getters
    public String getMessageId() { return messageId; }
    public String getRoomId() { return roomId; }
    public String getUserId() { return userId; }
    public String getUsername() { return username; }
    public String getMessage() { return message; }
    public String getTimestamp() { return timestamp; }
    public String getMessageType() { return messageType; }
    public String getServerId() { return serverId; }
    public String getClientIp() { return clientIp; }
}
