package v2x.coordinator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

public class MasterServer implements Runnable {

    private static final String CMD_REGISTER_PUB   = "REGISTER_PUB";
    private static final String CMD_UNREGISTER_PUB = "UNREGISTER_PUB";
    private static final String CMD_LOOKUP_PUBS    = "LOOKUP_PUBS";

    private final int port;

    // topic -> { (host,port) ... }
    private final ConcurrentMap<String, Set<Endpoint>> topicPubs = new ConcurrentHashMap<>();

    // ★固定サイズのワーカースレッドプール
    private final ExecutorService workerPool;

    public MasterServer(int port) {
        this.port = port;
        int workers = 16; // 必要ならここを増減してください
        this.workerPool = Executors.newFixedThreadPool(
                workers,
                r -> {
                    Thread t = new Thread(r, "MasterClientWorker");
                    t.setDaemon(true);
                    return t;
                }
        );
    }

    @Override
    public void run() {
        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("[Master] listen on " + server.getLocalPort());

            while (true) {
                Socket sock = server.accept();
                try {
                    // ★毎回 Thread を new せず、プールに投げる
                    workerPool.submit(() -> handleClient(sock));
                } catch (RejectedExecutionException ex) {
                    System.err.println("[Master] worker pool full, handle client inline");
                    handleClient(sock);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("MasterServer failed", e);
        } finally {
            workerPool.shutdownNow();
        }
    }

    private void handleClient(Socket sock) {
        try (sock;
             BufferedReader in = new BufferedReader(
                     new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(
                     new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8), true)) {

            String line = in.readLine();
            if (line == null) {
                return;
            }

            String[] parts = line.trim().split("\\s+");
            if (parts.length == 0) return;

            String cmd = parts[0];

            switch (cmd) {
                case CMD_REGISTER_PUB:
                    handleRegisterPub(parts, out);
                    break;
                case CMD_UNREGISTER_PUB:
                    handleUnregisterPub(parts, out);
                    break;
                case CMD_LOOKUP_PUBS:
                    handleLookupPubs(parts, out);
                    break;
                default:
                    out.println("ERROR unknown command");
                    break;
            }

        } catch (Exception e) {
            System.err.println("[Master] client handler error: " + e);
        }
    }

    private void handleRegisterPub(String[] parts, PrintWriter out) {
        // REGISTER_PUB <topic> <host> <port>
        if (parts.length != 4) {
            out.println("ERROR invalid REGISTER_PUB");
            return;
        }
        String topic = parts[1];
        String host  = parts[2];
        int    port  = Integer.parseInt(parts[3]);
        Endpoint ep = new Endpoint(host, port);

        topicPubs.compute(topic, (t, set) -> {
            if (set == null) {
                set = ConcurrentHashMap.newKeySet();
            }
            set.add(ep);
            return set;
        });

        out.println("OK");
    }

    private void handleUnregisterPub(String[] parts, PrintWriter out) {
        // UNREGISTER_PUB <topic> <host> <port>
        if (parts.length != 4) {
            out.println("ERROR invalid UNREGISTER_PUB");
            return;
        }
        String topic = parts[1];
        String host  = parts[2];
        int    port  = Integer.parseInt(parts[3]);
        Endpoint ep = new Endpoint(host, port);

        topicPubs.computeIfPresent(topic, (t, set) -> {
            set.remove(ep);
            return set.isEmpty() ? null : set;
        });

        out.println("OK");
    }

    private void handleLookupPubs(String[] parts, PrintWriter out) {
        // LOOKUP_PUBS <topic>
        if (parts.length != 2) {
            out.println("ERROR invalid LOOKUP_PUBS");
            return;
        }
        String topic = parts[1];

        Set<Endpoint> set = topicPubs.get(topic);
        if (set == null || set.isEmpty()) {
            out.println("ENDPOINTS");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ENDPOINTS");
        for (Endpoint ep : set) {
            sb.append(' ')
              .append(ep.host)
              .append(':')
              .append(ep.port);
        }
        out.println(sb.toString());
    }

    private static final class Endpoint {
        final String host;
        final int port;

        Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Endpoint)) return false;
            Endpoint endpoint = (Endpoint) o;
            return port == endpoint.port &&
                   Objects.equals(host, endpoint.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }
    }
}
