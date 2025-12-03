package v2x.vehicle.net;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ROS1 の TCPROS Publisher をイメージした簡易実装。
 *
 * - publish() されたメッセージを、接続中の Subscriber 全員に配信する
 * - 起動時に MasterServer に REGISTER_PUB する
 */
public class RosPublisher implements AutoCloseable {

    private final MasterClient master;
    private final String topic;
    private final String advertiseHost;
    private final int listenPort;

    private final CopyOnWriteArrayList<DataOutputStream> clients = new CopyOnWriteArrayList<>();
    private volatile boolean running = false;
    private Thread acceptThread;

    public RosPublisher(MasterClient master,
                        String topic,
                        String advertiseHost,
                        int listenPort) {
        this.master = master;
        this.topic = topic;
        this.advertiseHost = advertiseHost;
        this.listenPort = listenPort;
    }

    public void start() throws IOException {
        if (running) return;
        running = true;

        // Master への登録
        master.registerPublisher(topic, advertiseHost, listenPort);

        acceptThread = new Thread(() -> {
            try (ServerSocket ss = new ServerSocket(listenPort)) {
                System.out.println("[RosPublisher] listen on " + listenPort + " topic=" + topic);
                while (running) {
                    Socket sock = ss.accept();
                    System.out.println("[RosPublisher] subscriber connected from " + sock.getRemoteSocketAddress());
                    clients.add(new DataOutputStream(sock.getOutputStream()));
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("[RosPublisher] accept error: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }, "RosPublisher-accept-" + topic);
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void publish(byte[] payload) {
        if (!running) return;
        int len = payload.length;
        long now = System.currentTimeMillis();

        for (Iterator<DataOutputStream> it = clients.iterator(); it.hasNext(); ) {
            DataOutputStream out = it.next();
            try {
                out.writeInt(len);
                out.write(payload);
                out.flush();
            } catch (IOException e) {
                System.err.println("[RosPublisher] send error -> remove subscriber: " + e.getMessage());
                clients.remove(out);
                try {
                    out.close();
                } catch (IOException ignore) {
                }
            }
        }
    }

    @Override
    public void close() {
        running = false;
        if (acceptThread != null) {
            acceptThread.interrupt();
        }
        for (DataOutputStream out : clients) {
            try {
                out.close();
            } catch (IOException ignore) {
            }
        }
        clients.clear();
        try {
            master.unregisterPublisher(topic, advertiseHost, listenPort);
        } catch (IOException e) {
            System.err.println("[RosPublisher] unregister failed: " + e.getMessage());
        }
    }
}
