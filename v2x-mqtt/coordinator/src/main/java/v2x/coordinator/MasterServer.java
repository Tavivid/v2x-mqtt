package v2x.coordinator;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ROS1 の Master に似た役割をするシンプルなサーバ。
 *
 * プロトコル（1 リクエスト 1 コネクション）:
 *
 *   REGISTER_PUB <topic> <host> <port>
 *   UNREGISTER_PUB <topic> <host> <port>
 *   LOOKUP_PUBS <topic>
 *
 * レスポンス:
 *
 *   OK
 *   ENDPOINTS host1:port1,host2:port2,...
 */
public class MasterServer implements Runnable {

    public static final class Endpoint {
        public final String host;
        public final int port;

        public Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    private final int port;
    // topic -> endpoints
    private final Map<String, Set<Endpoint>> table = new ConcurrentHashMap<>();

    public MasterServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try (ServerSocket ss = new ServerSocket(port)) {
            System.out.println("[Master] started on port " + port);
            while (true) {
                Socket sock = ss.accept();
                new Thread(() -> handleClient(sock),
                           "MasterClient-" + sock.getRemoteSocketAddress()).start();
            }
        } catch (IOException e) {
            System.err.println("[Master] fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleClient(Socket sock) {
        try (sock;
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(sock.getOutputStream()), true)) {

            String line = in.readLine();
            if (line == null) {
                return;
            }
            String resp = process(line.trim());
            out.println(resp);
        } catch (IOException e) {
            // ログだけ出して終了
            System.err.println("[Master] client error: " + e.getMessage());
        }
    }

    private String process(String line) {
        try {
            String[] parts = line.split("\\s+");
            if (parts.length < 2) {
                return "ERR invalid-command";
            }
            String cmd = parts[0];
            switch (cmd) {
                case "REGISTER_PUB":
                    if (parts.length != 4) return "ERR usage: REGISTER_PUB <topic> <host> <port>";
                    return registerPub(parts[1], parts[2], Integer.parseInt(parts[3]));
                case "UNREGISTER_PUB":
                    if (parts.length != 4) return "ERR usage: UNREGISTER_PUB <topic> <host> <port>";
                    return unregisterPub(parts[1], parts[2], Integer.parseInt(parts[3]));
                case "LOOKUP_PUBS":
                    if (parts.length != 2) return "ERR usage: LOOKUP_PUBS <topic>";
                    return lookupPubs(parts[1]);
                default:
                    return "ERR unknown-command";
            }
        } catch (Exception e) {
            return "ERR " + e.getClass().getSimpleName() + ":" + e.getMessage();
        }
    }

    private String registerPub(String topic, String host, int port) {
        Endpoint ep = new Endpoint(host, port);
        table.compute(topic, (t, set) -> {
            if (set == null) set = ConcurrentHashMap.newKeySet();
            set.add(ep);
            return set;
        });
        System.out.println("[Master] REGISTER_PUB topic=" + topic + " ep=" + ep);
        return "OK";
    }

    private String unregisterPub(String topic, String host, int port) {
        Endpoint ep = new Endpoint(host, port);
        table.computeIfPresent(topic, (t, set) -> {
            set.remove(ep);
            return set.isEmpty() ? null : set;
        });
        System.out.println("[Master] UNREGISTER_PUB topic=" + topic + " ep=" + ep);
        return "OK";
    }

    private String lookupPubs(String topic) {
        Set<Endpoint> set = table.getOrDefault(topic, Collections.emptySet());
        if (set.isEmpty()) {
            return "ENDPOINTS";
        }
        StringBuilder sb = new StringBuilder("ENDPOINTS ");
        boolean first = true;
        for (Endpoint ep : set) {
            if (!first) sb.append(",");
            sb.append(ep.host).append(":").append(ep.port);
            first = false;
        }
        return sb.toString();
    }
}
