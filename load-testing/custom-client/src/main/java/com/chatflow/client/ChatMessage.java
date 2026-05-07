package com.chatflow.client;

public class ChatMessage {
    private String userId;
    private String username;
    private String message;
    private String timestamp;
    private String messageType;
    private transient int roomId; // transient: excluded from JSON serialization, used for routing only

    public String getUserId(){
        return userId;
    }

    public void setUserId(String newUserId){
        this.userId = newUserId;
    }

    public String getUsername(){
        return username;
    }

    public void setUsername(String newUsername){
        this.username = newUsername;
    }

    public String getMessage(){
        return message;
    }

    public void setMessage(String newMessage){
        this.message = newMessage;
    }

    public String getTimestamp(){
        return timestamp;
    }

    public void setTimestamp(String newTimestamp){
        this.timestamp = newTimestamp;
    }

    public String getMessageType(){
        return messageType;
    }

    public void setMessageType(String newMessageType){
        this.messageType = newMessageType;
    }

    public int getRoomId(){
        return roomId;
    }

    public void setRoomId(int newRoomId){
        this.roomId = newRoomId;
    }

}
