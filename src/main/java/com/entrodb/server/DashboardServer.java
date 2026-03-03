package com.entrodb.server;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class DashboardServer {

    public static final int DEFAULT_DASHBOARD_PORT = 6970;

    private final int            port;
    private final StatsCollector stats;
    private volatile boolean     running = false;
    private ServerSocket         serverSocket;
    private final ExecutorService pool =
        Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "dashboard-http");
            t.setDaemon(true);
            return t;
        });

    public DashboardServer(int port, StatsCollector stats) {
        this.port  = port;
        this.stats = stats;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setReuseAddress(true);
        running = true;
        System.out.printf("  Dashboard:    http://localhost:%d%n", port);
        Thread t = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    pool.submit(() -> handleRequest(client));
                } catch (IOException e) {
                    if (running)
                        System.err.println("Dashboard error: " + e.getMessage());
                }
            }
        }, "dashboard-accept");
        t.setDaemon(true);
        t.start();
    }

    public void stop() {
        running = false;
        try { if (serverSocket != null) serverSocket.close(); }
        catch (IOException ignored) {}
        pool.shutdown();
    }

    private void handleRequest(Socket client) {
        try (
            BufferedReader in = new BufferedReader(
                new InputStreamReader(client.getInputStream()));
            OutputStream out = client.getOutputStream()
        ) {
            String requestLine = in.readLine();
            if (requestLine == null) return;
            String line;
            while ((line = in.readLine()) != null && !line.isEmpty()) {}
            String path = requestLine.split(" ")[1].split("\\?")[0];
            if (path.equals("/api/stats")) serveJSON(out);
            else                           serveHTML(out);
        } catch (IOException ignored) {
        } finally {
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    private void serveJSON(OutputStream out) throws IOException {
        String json = toJSON(stats.snapshot());
        writeHTTP(out, "application/json", json);
    }

    private void serveHTML(OutputStream out) throws IOException {
        writeHTTP(out, "text/html; charset=utf-8", buildHTML());
    }

    private void writeHTTP(OutputStream out, String ct, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        String header = "HTTP/1.1 200 OK\r\n"
            + "Content-Type: " + ct + "\r\n"
            + "Access-Control-Allow-Origin: *\r\n"
            + "Content-Length: " + bytes.length + "\r\n\r\n";
        out.write(header.getBytes(StandardCharsets.UTF_8));
        out.write(bytes);
        out.flush();
    }

    @SuppressWarnings("unchecked")
    private String toJSON(Object obj) {
        if (obj == null)             return "null";
        if (obj instanceof String s) return "\"" + escape(s) + "\"";
        if (obj instanceof Boolean b) return b.toString();
        if (obj instanceof Number n)  return n.toString();
        if (obj instanceof List<?> list) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(toJSON(list.get(i)));
            }
            return sb.append("]").toString();
        }
        if (obj instanceof Map<?,?> map) {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<?,?> e : map.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(escape(e.getKey().toString()))
                  .append("\":").append(toJSON(e.getValue()));
                first = false;
            }
            return sb.append("}").toString();
        }
        return "\"" + escape(obj.toString()) + "\"";
    }

    private String escape(String s) {
        return s.replace("\\","\\\\").replace("\"","\\\"")
                .replace("\n","\\n").replace("\r","\\r")
                .replace("\t","\\t");
    }

    private String buildHTML() {
        return "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
+ "<meta name='viewport' content='width=device-width,initial-scale=1'>"
+ "<title>EntroDB Admin</title><style>"
+ "*{margin:0;padding:0;box-sizing:border-box}"
+ "body{font-family:'Segoe UI',system-ui,sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}"
+ "header{background:linear-gradient(135deg,#1a1d2e,#16213e);border-bottom:1px solid #2d3748;padding:16px 32px;display:flex;align-items:center;justify-content:space-between}"
+ "header h1{font-size:22px;font-weight:700;background:linear-gradient(135deg,#667eea,#764ba2);-webkit-background-clip:text;-webkit-text-fill-color:transparent}"
+ "#dot{width:10px;height:10px;border-radius:50%;background:#48bb78;box-shadow:0 0 8px #48bb78;display:inline-block;margin-right:8px;animation:pulse 2s infinite}"
+ "@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}"
+ ".wrap{padding:24px 32px;max-width:1400px;margin:0 auto}"
+ ".g4{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}"
+ ".g2{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px}"
+ ".card{background:#1a1d2e;border:1px solid #2d3748;border-radius:12px;padding:20px}"
+ ".card h3{font-size:11px;text-transform:uppercase;letter-spacing:1.5px;color:#718096;margin-bottom:8px}"
+ ".val{font-size:32px;font-weight:700;color:#e2e8f0;line-height:1}"
+ ".sub{font-size:12px;color:#718096;margin-top:4px}"
+ ".blue .val{color:#63b3ed}.green .val{color:#68d391}.purple .val{color:#b794f4}.orange .val{color:#f6ad55}"
+ ".st{font-size:14px;font-weight:600;color:#a0aec0;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #2d3748}"
+ "table{width:100%;border-collapse:collapse;font-size:13px}"
+ "th{text-align:left;padding:8px 12px;color:#718096;font-size:11px;text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid #2d3748}"
+ "td{padding:10px 12px;border-bottom:1px solid #1e2233;color:#cbd5e0}"
+ "tr:last-child td{border-bottom:none}tr:hover td{background:#1e2233}"
+ ".badge{display:inline-block;padding:2px 8px;border-radius:9999px;font-size:11px;font-weight:600}"
+ ".ok{background:#1c4532;color:#68d391}.err{background:#742a2a;color:#fc8181}.bl{background:#1a365d;color:#63b3ed}"
+ ".bar{background:#2d3748;border-radius:4px;height:6px;overflow:hidden;margin-top:8px}"
+ ".fill{height:100%;border-radius:4px;background:linear-gradient(90deg,#667eea,#764ba2);transition:width .5s ease}"
+ ".sql{font-family:'Courier New',monospace;font-size:12px;color:#90cdf4;max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}"
+ "#lu{font-size:11px;color:#4a5568}"
+ "</style></head><body>"
+ "<header><div><span id='dot'></span><h1 style='display:inline'>EntroDB Admin Dashboard</h1></div>"
+ "<span id='lu'>Updating...</span></header>"
+ "<div class='wrap'>"
+ "<div class='g4'>"
+ "<div class='card blue'><h3>Total Queries</h3><div class='val' id='tq'>—</div><div class='sub' id='te'>— errors</div></div>"
+ "<div class='card green'><h3>Active Clients</h3><div class='val' id='ac'>—</div><div class='sub'>connected sessions</div></div>"
+ "<div class='card purple'><h3>Active Transactions</h3><div class='val' id='at'>—</div><div class='sub' id='al'>— locks held</div></div>"
+ "<div class='card orange'><h3>Uptime</h3><div class='val' id='up'>—</div><div class='sub'>seconds</div></div>"
+ "</div>"
+ "<div class='g2'>"
+ "<div class='card'><div class='st'>Buffer Pool</div>"
+ "<div style='display:flex;justify-content:space-between;margin-bottom:4px'><span style='font-size:13px;color:#a0aec0'>Used Frames</span><span style='font-size:13px' id='bu'>—</span></div>"
+ "<div class='bar'><div class='fill' id='bb' style='width:0%'></div></div>"
+ "<div style='display:flex;justify-content:space-between;margin-top:12px'><span style='font-size:12px;color:#718096'>Pool size</span><span style='font-size:12px;color:#718096' id='bs'>—</span></div>"
+ "<div style='display:flex;justify-content:space-between;margin-top:4px'><span style='font-size:12px;color:#718096'>Dirty pages</span><span style='font-size:12px;color:#f6ad55' id='bd'>—</span></div></div>"
+ "<div class='card'><div class='st'>Tables</div>"
+ "<table><thead><tr><th>Name</th><th>Columns</th><th>Indexes</th></tr></thead>"
+ "<tbody id='tb'><tr><td colspan='3' style='color:#4a5568'>No tables</td></tr></tbody></table></div>"
+ "</div>"
+ "<div class='card'><div class='st'>Recent Queries</div>"
+ "<table><thead><tr><th>SQL</th><th>User</th><th>Duration</th><th>Status</th><th>Time</th></tr></thead>"
+ "<tbody id='qb'><tr><td colspan='5' style='color:#4a5568'>No queries yet</td></tr></tbody></table></div>"
+ "</div>"
+ "<script>"
+ "function fu(s){if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s';return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m'}"
+ "function ta(ts){const d=Math.floor((Date.now()-ts)/1000);if(d<5)return 'just now';if(d<60)return d+'s ago';return Math.floor(d/60)+'m ago'}"
+ "async function refresh(){"
+ "try{"
+ "const r=await fetch('/api/stats');const d=await r.json();"
+ "document.getElementById('tq').textContent=d.total_queries;"
+ "document.getElementById('te').textContent=d.total_errors+' errors';"
+ "document.getElementById('ac').textContent=d.active_clients;"
+ "document.getElementById('at').textContent=d.active_txns;"
+ "document.getElementById('al').textContent=d.active_locks+' locks held';"
+ "document.getElementById('up').textContent=fu(d.uptime_seconds);"
+ "const pct=d.buffer_pool_size>0?Math.round(d.buffer_pool_used/d.buffer_pool_size*100):0;"
+ "document.getElementById('bu').textContent=d.buffer_pool_used+' / '+d.buffer_pool_size;"
+ "document.getElementById('bs').textContent=d.buffer_pool_size;"
+ "document.getElementById('bd').textContent=d.buffer_dirty;"
+ "document.getElementById('bb').style.width=pct+'%';"
+ "const tb=document.getElementById('tb');"
+ "tb.innerHTML=d.tables.length===0?'<tr><td colspan=3 style=color:#4a5568>No tables</td></tr>'"
+ ":d.tables.map(t=>`<tr><td>${t.name}</td><td><span class=badge bl>${t.columns}</span></td><td><span class=badge ok>${t.indexes}</span></td></tr>`).join('');"
+ "const qb=document.getElementById('qb');"
+ "qb.innerHTML=d.recent_queries.length===0?'<tr><td colspan=5 style=color:#4a5568>No queries yet</td></tr>'"
+ ":d.recent_queries.map(q=>`<tr><td class=sql title='${q.sql}'>${q.sql}</td><td>${q.user}</td><td>${q.ms}ms</td><td><span class='badge ${q.error?'err':'ok'}'>${q.error?'ERROR':'OK'}</span></td><td style='color:#4a5568;font-size:12px'>${ta(q.time)}</td></tr>`).join('');"
+ "document.getElementById('lu').textContent='Updated '+new Date().toLocaleTimeString();"
+ "document.getElementById('dot').style.background='#48bb78';"
+ "}catch(e){"
+ "document.getElementById('dot').style.background='#fc8181';"
+ "document.getElementById('lu').textContent='Connection lost';}}"
+ "refresh();setInterval(refresh,2000);"
+ "</script></body></html>";
    }
}
