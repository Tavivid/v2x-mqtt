package v2x.vehicle.net;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * ROS1 の TCPROS Subscriber に相当する簡易実装。
 *
 * - Master に LOOKUP_PUBS して Publisher 一覧を取得
 * - 各 Publisher に TCP 接続し、長さ付きフレーム (int length + payload) を読み続ける
 */
public class RosSubscriber implements AutoCloseable {

    private final MasterClient master;
    private final String topic;
    private final Consumer<byte[]> callback;

    private final CopyOnWriteArrayList<Thread> receiverThreads = new CopyOnWriteArrayList<>();
    private volatile boolean running = false;

    public RosSubscriber(MasterClient master,
                         String topic,
                         Consumer<byte[]> callback) {
        this.master = master;
        this.topic = topic;
        this.callback = callback;
    }

    public void start() throws IOException {
        if (running) return;
        running = true;

        List<MasterClient.Endpoint> eps = master.lookupPublishers(topic);
        if (eps.isEmpty()) {
            System.out.println("[RosSubscriber] no publishers for topic=" + topic);
        }
        for (MasterClient.Endpoint ep : eps) {
            startReceiver(ep);
        }
    }

    private void startReceiver(MasterClient.Endpoint ep) {
        Thread t = new Thread(() -> {
            while (running) {
                try (Socket sock = new Socket(ep.host, ep.port);
                     DataInputStream in = new DataInputStream(sock.getInputStream())) {

                    System.out.println("[RosSubscriber] connected to publisher " + ep);
                    while (running) {
                        int len;
                        try {
                            len = in.readInt();
                        } catch (EOFException e) {
                            break;
                        }
                        if (len <= 0) {
                            continue;
                        }
                        byte[] buf = new byte[len];
                        in.readFully(buf);
                        callback.accept(buf);
                    }
                } catch (IOException e) {
                    if (!running) break;
                    System.err.println("[RosSubscriber] receive error from " + ep + ": " + e.getMessage());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, "RosSubscriber-" + topic + "-" + ep);
        t.setDaemon(true);
        t.start();
        receiverThreads.add(t);
    }

    @Override
    public void close() {
        running = false;
        for (Thread t : receiverThreads) {
            t.interrupt();
        }
        receiverThreads.clear();
    }
}
