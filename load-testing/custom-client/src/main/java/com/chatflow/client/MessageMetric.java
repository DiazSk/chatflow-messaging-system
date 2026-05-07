package com.chatflow.client;

public class MessageMetric {
    private final long sendTimestamp;      // When we sent
    private final long receiveTimestamp;   // When we got response
    private final long latencyMs;          // Difference
    private final String messageType;      // TEXT, JOIN, LEAVE
    private final String status;           // success or error
    private final int roomId;              // Room the message was sent to

    public MessageMetric(long sendTimestamp, long receiveTimestamp, String messageType, String status, int roomId) {
        this.sendTimestamp = sendTimestamp;
        this.receiveTimestamp = receiveTimestamp;
        this.latencyMs = receiveTimestamp - sendTimestamp;
        this.messageType = messageType;
        this.status = status;
        this.roomId = roomId;
    }

    // Getters
    public long getSendTimestamp() { return sendTimestamp; }
    public long getReceiveTimestamp() { return receiveTimestamp; }
    public long getLatencyMs() { return latencyMs; }
    public String getMessageType() { return messageType; }
    public String getStatus() { return status; }
    public int getRoomId() { return roomId; }
}
