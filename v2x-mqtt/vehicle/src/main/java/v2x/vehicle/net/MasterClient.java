package v2x.vehicle.net;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Coordinator 側の MasterServer としゃべるクライアント。
 *
 * プロトコル:
 *
 *   REGISTER_PUB <topic> <host> <port>
 *   UNREGISTER_PUB <topic> <host> <port>
 *   LOOKUP_PUBS <topic>
 *
 * レスポンス:
 *   OK
 *   ENDPOINTS host1:port1,host2:port2,...
 */
public class MasterClient {

    public static final class Endpoint {
        public final String host;
        public final int port;

        public Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public static Endpoint parse(String s) {
            String[] hp = s.split(":");
            if (hp.length != 2) {
                throw new IllegalArgumentException("invalid endpoint: " + s);
            }
            return new Endpoint(hp[0], Integer.parseInt(hp[1]));
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Endpoint)) return false;
            Endpoint other = (Endpoint) o;
            return this.host.equals(other.host) && this.port == other.port;
        }

        @Override
        public int hashCode() {
            return host.hashCode() * 31 + port;
        }
    }

    private final String host;
    private final int port;

    public MasterClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void registerPublisher(String topic, String pubHost, int pubPort) throws IOException {
        String cmd = String.format("REGISTER_PUB %s %s %d", topic, pubHost, pubPort);
        String resp = sendCommand(cmd);
        if (!resp.startsWith("OK")) {
            throw new IOException("registerPublisher failed: " + resp);
        }
    }

    public void unregisterPublisher(String topic, String pubHost, int pubPort) throws IOException {
        String cmd = String.format("UNREGISTER_PUB %s %s %d", topic, pubHost, pubPort);
        String resp = sendCommand(cmd);
        if (!resp.startsWith("OK")) {
            throw new IOException("unregisterPublisher failed: " + resp);
        }
    }

    public List<Endpoint> lookupPublishers(String topic) throws IOException {
        String cmd = String.format("LOOKUP_PUBS %s", topic);
        String resp = sendCommand(cmd);
        if (!resp.startsWith("ENDPOINTS")) {
            throw new IOException("lookupPublishers failed: " + resp);
        }

        String rest = resp.substring("ENDPOINTS".length()).trim();
        List<Endpoint> list = new ArrayList<>();
        if (!rest.isEmpty()) {
            String[] eps = rest.split(",");
            for (String epStr : eps) {
                if (!epStr.isEmpty()) {
                    list.add(Endpoint.parse(epStr));
                }
            }
        }
        return list;
    }

    private String sendCommand(String cmd) throws IOException {
        try (Socket sock = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(sock.getOutputStream()), true)) {

            out.println(cmd);
            String resp = in.readLine();
            if (resp == null) {
                throw new IOException("master closed connection without response");
            }
            return resp;
        }
    }
}
