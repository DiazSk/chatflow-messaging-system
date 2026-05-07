package com.chatflow.processor;

/**
 * Message format as published by the gateway-server to RabbitMQ.
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

    // Add these below your Getters
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public void setRoomId(String roomId) { this.roomId = roomId; }
    public void setUserId(String userId) { this.userId = userId; }
    public void setUsername(String username) { this.username = username; }
    public void setMessage(String message) { this.message = message; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setMessageType(String messageType) { this.messageType = messageType; }
    public void setServerId(String serverId) { this.serverId = serverId; }
    public void setClientIp(String clientIp) { this.clientIp = clientIp; }
}
