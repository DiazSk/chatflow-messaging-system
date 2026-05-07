package com.chatflow.processor;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 * WebSocket server running on the consumer side.
 * Clients connect here to receive broadcast messages for their room.
 * 
 * When a client connects to /chat/room{id}, they are registered in the RoomManager.
 * The MessageConsumerPool then broadcasts queue messages to all sessions in each room.
 */
public class BroadcastServer extends WebSocketServer {

    private final RoomManager roomManager;
    private final AtomicLong totalConnections = new AtomicLong(0);

    public BroadcastServer(int port, RoomManager roomManager) {
        super(new InetSocketAddress(port));
        this.roomManager = roomManager;
        this.setTcpNoDelay(true);
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String resourceDescriptor = conn.getResourceDescriptor();
        String roomId = extractRoomId(resourceDescriptor);
        
        // Register in RoomManager (userId and username will be set when first message arrives)
        // For now, use connection address as placeholder
        String connId = conn.getRemoteSocketAddress() != null 
                ? conn.getRemoteSocketAddress().toString() : "unknown";
        
        roomManager.joinRoom(roomId, conn, connId, "");
        conn.setAttachment(roomId);
        
        totalConnections.incrementAndGet();
        System.out.println("Broadcast client connected: " + connId + " | room: " + roomId 
                + " | total rooms: " + roomManager.getActiveRoomCount()
                + " | total connections: " + roomManager.getTotalConnections());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        // The broadcast server primarily sends messages TO clients, not receiving FROM them.
        // However, clients might send identification or heartbeat messages.
        // For now, we just log it.
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        roomManager.leaveRoom(conn);
        System.out.println("Broadcast client disconnected: " + conn.getRemoteSocketAddress());
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        System.err.println("BroadcastServer error: " + ex.getMessage());
        if (conn != null) {
            roomManager.leaveRoom(conn);
        }
    }

    @Override
    public void onStart() {
        System.out.println("BroadcastServer started on port: " + getPort());
    }

    private String extractRoomId(String resourceDescriptor) {
        if (resourceDescriptor == null) return "1";
        
        if (resourceDescriptor.startsWith("/chat/room")) {
            return resourceDescriptor.substring("/chat/room".length());
        } else if (resourceDescriptor.startsWith("/chat/")) {
            return resourceDescriptor.substring("/chat/".length());
        }
        return "1";
    }

    public long getTotalConnections() { return totalConnections.get(); }
}
