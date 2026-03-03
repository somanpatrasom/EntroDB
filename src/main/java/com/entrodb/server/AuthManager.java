package com.entrodb.server;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple username/password authentication.
 * Passwords stored as SHA-256 hashes in data/users.auth
 *
 * Default user: admin / entrodb
 * Change with: CREATE USER username PASSWORD 'password'
 */
public class AuthManager {

    private final Path authFile;
    private final Map<String, String> users = new ConcurrentHashMap<>();

    public AuthManager(String dataDir) throws IOException {
        this.authFile = Paths.get(dataDir, "users.auth");
        if (Files.exists(authFile)) {
            load();
        } else {
            // Create default admin user
            addUser("admin", "entrodb");
            System.out.println("AUTH: default user created — admin/entrodb");
            System.out.println("AUTH: change password immediately in production!");
        }
    }

    public boolean authenticate(String username, String password) {
        String stored = users.get(username.toLowerCase());
        if (stored == null) return false;
        return stored.equals(hash(password));
    }

    public void addUser(String username, String password) throws IOException {
        users.put(username.toLowerCase(), hash(password));
        save();
    }

    public void removeUser(String username) throws IOException {
        users.remove(username.toLowerCase());
        save();
    }

    public void changePassword(String username, String newPassword)
            throws IOException {
        if (!users.containsKey(username.toLowerCase()))
            throw new RuntimeException("User not found: " + username);
        users.put(username.toLowerCase(), hash(newPassword));
        save();
    }

    public boolean userExists(String username) {
        return users.containsKey(username.toLowerCase());
    }

    // ── Helpers ───────────────────────────────────────────────────

    private String hash(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(password.getBytes("UTF-8"));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest)
                sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("Hash failed", e);
        }
    }

    private void save() throws IOException {
        List<String> lines = new ArrayList<>();
        for (Map.Entry<String, String> e : users.entrySet())
            lines.add(e.getKey() + ":" + e.getValue());
        Files.write(authFile, lines);
    }

    private void load() throws IOException {
        for (String line : Files.readAllLines(authFile)) {
            if (line.isBlank()) continue;
            String[] parts = line.split(":", 2);
            if (parts.length == 2) users.put(parts[0], parts[1]);
        }
        System.out.println("AUTH: loaded " + users.size() + " user(s)");
    }
}
