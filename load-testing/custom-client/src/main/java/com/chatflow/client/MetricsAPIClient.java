package com.chatflow.client;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Client-side utility to call the Metrics API after test completion.
 *
 * Uses very generous timeouts (180s) because on EC2 t3.micro,
 * the single vCPU may still be finishing DB writes when we call.
 * Individual endpoints are called first to warm the cache before /api/all.
 */
public class MetricsAPIClient {

    private final String metricsBaseUrl;

    // Very generous timeouts for EC2 t3.micro under load
    private static final int CONNECT_TIMEOUT_MS = 30_000;    // 30s connect
    private static final int READ_TIMEOUT_MS = 180_000;      // 180s read (analytics on millions of rows)
    private static final int MAX_RETRIES = 3;                // Retry failed calls
    private static final int RETRY_DELAY_MS = 10_000;        // 10s between retries

    public MetricsAPIClient(String metricsBaseUrl) {
        this.metricsBaseUrl = metricsBaseUrl;
    }

    public void fetchAndLogAllResults() {
        String logFile = "metrics-api-results-" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".log";

        try (PrintWriter fileOut = new PrintWriter(new FileWriter(logFile))) {
            log(fileOut, "\n" + "=".repeat(80));
            log(fileOut, "=== METRICS API RESULTS (POST-TEST) ===");
            log(fileOut, "=".repeat(80));

            log(fileOut, "\n--- /api/health ---");
            log(fileOut, formatJson(callWithRetry("/api/health")));

            log(fileOut, "\n--- /api/metrics ---");
            log(fileOut, formatJson(callWithRetry("/api/metrics")));

            log(fileOut, "\n--- Core Query 1: Room Messages (room 1) ---");
            log(fileOut, formatJson(callWithRetry("/api/messages/room?roomId=1&start=2020-01-01+00:00:00&end=2030-12-31+23:59:59")));

            log(fileOut, "\n--- Core Query 2: User History (user 1) ---");
            log(fileOut, formatJson(callWithRetry("/api/messages/user?userId=1")));

            log(fileOut, "\n--- Core Query 3: Active Users ---");
            log(fileOut, formatJson(callWithRetry("/api/users/active?start=2020-01-01+00:00:00&end=2030-12-31+23:59:59")));

            log(fileOut, "\n--- Core Query 4: User Rooms (user 1) ---");
            log(fileOut, formatJson(callWithRetry("/api/users/rooms?userId=1")));

            log(fileOut, "\n--- Analytics: Messages Per Minute ---");
            log(fileOut, formatJson(callWithRetry("/api/analytics/throughput?limit=10")));

            log(fileOut, "\n--- Analytics: Top 10 Users ---");
            log(fileOut, formatJson(callWithRetry("/api/analytics/top-users?n=10")));

            log(fileOut, "\n--- Analytics: Top 10 Rooms ---");
            log(fileOut, formatJson(callWithRetry("/api/analytics/top-rooms?n=10")));

            log(fileOut, "\n--- Analytics: Participation Patterns ---");
            log(fileOut, formatJson(callWithRetry("/api/analytics/patterns")));

            log(fileOut, "\n--- /api/all (cached) ---");
            log(fileOut, formatJson(callWithRetry("/api/all")));

            log(fileOut, "\n" + "=".repeat(80));
            log(fileOut, "=== END METRICS API RESULTS ===");
            log(fileOut, "=".repeat(80));

            System.out.println("\nMetrics log saved to: " + logFile);
        } catch (IOException e) {
            System.err.println("Warning: could not write metrics log file: " + e.getMessage());
        }
    }

    private void log(PrintWriter fileOut, String message) {
        System.out.println(message);
        fileOut.println(message);
        fileOut.flush();
    }

    /**
     * Call an endpoint with retries. On EC2 t3.micro the first attempt
     * may fail if CPU is still busy from writes. Retries with 10s delay.
     */
    private String callWithRetry(String path) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            String result = callEndpoint(path);
            if (!result.contains("\"error\"")) {
                return result;
            }
            if (attempt < MAX_RETRIES) {
                System.out.println("  (Retry " + attempt + "/" + MAX_RETRIES + " for " + path + " in " + (RETRY_DELAY_MS/1000) + "s...)");
                try { Thread.sleep(RETRY_DELAY_MS); } catch (InterruptedException e) { break; }
            }
        }
        // Final attempt
        return callEndpoint(path);
    }

    private String callEndpoint(String path) {
        try {
            URL url = new URL(metricsBaseUrl + path);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            conn.setReadTimeout(READ_TIMEOUT_MS);

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                return "{\"error\":\"HTTP " + responseCode + "\"}";
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            return response.toString();

        } catch (Exception e) {
            return "{\"error\":\"" + e.getMessage() + "\"}";
        }
    }

    private String formatJson(String json) {
        if (json == null || json.isEmpty()) return "{}";

        StringBuilder sb = new StringBuilder();
        int indent = 0;
        boolean inString = false;

        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);

            if (c == '"' && (i == 0 || json.charAt(i - 1) != '\\')) {
                inString = !inString;
                sb.append(c);
                continue;
            }

            if (inString) {
                sb.append(c);
                continue;
            }

            switch (c) {
                case '{':
                case '[':
                    sb.append(c).append('\n');
                    indent++;
                    addIndent(sb, indent);
                    break;
                case '}':
                case ']':
                    sb.append('\n');
                    indent--;
                    addIndent(sb, indent);
                    sb.append(c);
                    break;
                case ',':
                    sb.append(c).append('\n');
                    addIndent(sb, indent);
                    break;
                case ':':
                    sb.append(c).append(' ');
                    break;
                default:
                    sb.append(c);
            }
        }

        return sb.toString();
    }

    private void addIndent(StringBuilder sb, int level) {
        for (int i = 0; i < level; i++) {
            sb.append("  ");
        }
    }
}
