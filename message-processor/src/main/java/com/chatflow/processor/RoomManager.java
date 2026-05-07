package com.chatflow.processor;

import org.java_websocket.WebSocket;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe manager for room memberships and user sessions.
 * 
 * Tracks:
 *   - Which WebSocket sessions are in each room
 *   - User info per session
 *   - Broadcast metrics
 */
public class RoomManager {

    // roomId -> set of WebSocket connections in that room
    private final ConcurrentHashMap<String, Set<WebSocket>> roomSessions = new ConcurrentHashMap<>();

    // WebSocket connection -> UserInfo
    private final ConcurrentHashMap<WebSocket, UserInfo> activeUsers = new ConcurrentHashMap<>();

    // Metrics
    private final AtomicLong messagesBroadcast = new AtomicLong(0);
    private final AtomicLong broadcastFailures = new AtomicLong(0);

    /**
     * Simple user info container.
     */
    public static class UserInfo {
        private final String roomId;
        private final String userId;
        private final String username;

        public UserInfo(String roomId, String userId, String username) {
            this.roomId = roomId;
            this.userId = userId;
            this.username = username;
        }

        public String getRoomId() { return roomId; }
        public String getUserId() { return userId; }
        public String getUsername() { return username; }
    }

    /**
     * Register a WebSocket connection to a room.
     */
    public void joinRoom(String roomId, WebSocket conn, String userId, String username) {
        // Add to room sessions set
        roomSessions.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(conn);
        
        // Track user info
        activeUsers.put(conn, new UserInfo(roomId, userId, username));
    }

    /**
     * Remove a WebSocket connection from its room.
     */
    public void leaveRoom(WebSocket conn) {
        UserInfo info = activeUsers.remove(conn);
        if (info != null) {
            Set<WebSocket> sessions = roomSessions.get(info.getRoomId());
            if (sessions != null) {
                sessions.remove(conn);
                // Clean up empty rooms
                if (sessions.isEmpty()) {
                    roomSessions.remove(info.getRoomId());
                }
            }
        }
    }

    /**
     * Broadcast a message to all connected sessions in a room.
     * 
     * @param roomId  the room to broadcast to
     * @param message the JSON message string to send
     * @return number of sessions the message was sent to
     */
    public int broadcastToRoom(String roomId, String message) {
        Set<WebSocket> sessions = roomSessions.get(roomId);
        if (sessions == null || sessions.isEmpty()) {
            return 0;
        }

        int sentCount = 0;
        for (WebSocket conn : sessions) {
            try {
                if (conn.isOpen()) {
                    conn.send(message);
                    sentCount++;
                } else {
                    // Clean up dead connections
                    sessions.remove(conn);
                    activeUsers.remove(conn);
                }
            } catch (Exception e) {
                broadcastFailures.incrementAndGet();
                // Remove failed connection
                sessions.remove(conn);
                activeUsers.remove(conn);
            }
        }

        if (sentCount > 0) {
            messagesBroadcast.incrementAndGet();
        }
        return sentCount;
    }

    /**
     * Get all sessions in a room (unmodifiable view).
     */
    public Set<WebSocket> getRoomSessions(String roomId) {
        Set<WebSocket> sessions = roomSessions.get(roomId);
        return sessions != null ? Collections.unmodifiableSet(sessions) : Collections.emptySet();
    }

    /**
     * Get user info for a connection.
     */
    public UserInfo getUserInfo(WebSocket conn) {
        return activeUsers.get(conn);
    }

    // Metrics
    public int getActiveRoomCount() { return roomSessions.size(); }
    
    public int getTotalConnections() { 
        return roomSessions.values().stream().mapToInt(Set::size).sum(); 
    }

    public long getMessagesBroadcast() { return messagesBroadcast.get(); }
    public long getBroadcastFailures() { return broadcastFailures.get(); }
}
