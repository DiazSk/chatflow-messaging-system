package com.chatflow.gateway;

public class ServerResponse {
    private ChatMessage originalMessage;
    private String serverTimestamp;
    private String status;
    private String errorMessage;

    public ServerResponse(ChatMessage originalMessage, String serverTimestamp, String status, String errorMessage) {
        this.originalMessage = originalMessage;
        this.serverTimestamp = serverTimestamp;
        this.status = status;
        this.errorMessage = errorMessage;
    }

    public ChatMessage getOriginalMessage() { return originalMessage; }
    public String getServerTimestamp() { return serverTimestamp; }
    public String getStatus() { return status; }
    public String getErrorMessage() { return errorMessage; }
}
