package com.chatflow.client;

import java.util.concurrent.BlockingQueue;
import java.util.Random;
import java.time.Instant;

public class MessageGenerator implements Runnable {

    private final BlockingQueue<ChatMessage> queue;
    private final int totalMessages;

    public MessageGenerator(BlockingQueue<ChatMessage> queue, int totalMessages) {
        this.queue = queue;
        this.totalMessages = totalMessages;
    }

    @Override
    public void run() {

        Random random = new Random();

        // Pool of 50 pre-defined messages
        String[] messagePool = {
            "Hello!", "How are you?", "Nice to meet you!", "How you doing?", "Cool!",
            "Good to see you!", "What's up?", "Hey there!", "Good morning!", "Good evening!",
            "How's it going?", "Long time no see!", "Great to chat!", "Welcome!", "Goodbye!",
            "See you later!", "Thanks!", "You're welcome!", "Sounds good!", "I agree!",
            "That's interesting!", "Tell me more!", "Really?", "No way!", "Absolutely!",
            "Of course!", "Let me think about it.", "I'll get back to you.", "Cheers!", "Take care!",
            "Have a great day!", "Talk soon!", "LOL!", "That's funny!", "I'm doing well!",
            "Not bad!", "Pretty good!", "Can't complain!", "Busy day today.", "Same here!",
            "Me too!", "I understand.", "Good point!", "Well said!", "Exactly!",
            "Right on!", "For sure!", "Definitely!", "Sounds like a plan!", "Let's do it!"
        };

        for (int i = 0; i < totalMessages; i++){
            // Generate a random user ID between 1 and 100000
            int userId = random.nextInt(100000) + 1;

            // Generate a username from userId
            String username = "user" + userId;

            // Generate a random room ID between 1 and 20
            int roomId = random.nextInt(20) + 1;

            // Generate a random message from pool
            String message = messagePool[random.nextInt(messagePool.length)];

            // Generate current timestamp
            String timestamp = Instant.now().toString();

            // Generate messageType (90% TEXT, 5% JOIN, 5% LEAVE)
            int roll = random.nextInt(100);
            String messageType;
            if (roll < 90) {
                messageType = "TEXT";
            } else if (roll < 95) {
                messageType = "JOIN";
            } else {
                messageType = "LEAVE";
            }

            // Create a new ChatMessage object
            ChatMessage chatMessage = new ChatMessage();
            chatMessage.setUserId(String.valueOf(userId));
            chatMessage.setUsername(username);
            chatMessage.setMessage(message);
            chatMessage.setTimestamp(timestamp);
            chatMessage.setMessageType(messageType);
            chatMessage.setRoomId(roomId);

            try {
                queue.put(chatMessage);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }
}
