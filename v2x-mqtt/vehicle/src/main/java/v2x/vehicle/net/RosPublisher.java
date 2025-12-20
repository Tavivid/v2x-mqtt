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
 * - publish() されたメッセージを、接続中の Subscriber 全員に配信する - 起動時に MasterServer に
 * REGISTER_PUB する
 */
public class RosPublisher implements AutoCloseable {

    private final MasterClient master;
    private final String topic;
    private final String advertiseHost;
    private final int listenPort;

    private final CopyOnWriteArrayList<DataOutputStream> clients = new CopyOnWriteArrayList<>();

    private volatile boolean running = false;
    private Thread acceptThread;
    private volatile ServerSocket serverSocket; // ★ 追加: listen ソケットを保持

    public RosPublisher(MasterClient master, String topic, String advertiseHost, int listenPort) {
        this.master = master;
        this.topic = topic;
        this.advertiseHost = advertiseHost;
        this.listenPort = listenPort;
    }

    public boolean hasSubscribers() {
        return !clients.isEmpty();
    }

    public synchronized void start() throws IOException {
        if (running) {
            return;
        }
        running = true;

        // Master への登録
        master.registerPublisher(topic, advertiseHost, listenPort);

        acceptThread = new Thread(() -> {
            try {
                ServerSocket ss = new ServerSocket(listenPort);
                serverSocket = ss;
                System.out.println("[RosPublisher] listen on " + listenPort + " topic=" + topic);
                while (running) {
                    try {
                        Socket sock = ss.accept();
                        System.out.println("[RosPublisher] subscriber connected from " + sock.getRemoteSocketAddress());
                        clients.add(new DataOutputStream(sock.getOutputStream()));
                    } catch (IOException e) {
                        // close() で serverSocket を閉じたときもここに来る
                        if (running) {
                            System.err.println("[RosPublisher] accept error: " + e.getMessage());
                            e.printStackTrace();
                        }
                        // running が false なら閉じるための例外なのでループを抜ける
                        break;
                    }
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("[RosPublisher] listen failed on port " + listenPort + " : " + e.getMessage());
                    e.printStackTrace();
                }
            } finally {
                // 終了時にソケットを必ず閉じる
                ServerSocket ss = serverSocket;
                serverSocket = null;
                if (ss != null && !ss.isClosed()) {
                    try {
                        ss.close();
                    } catch (IOException ignore) {
                    }
                }
            }
        }, "RosPublisher-accept-" + topic);

        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void publish(byte[] payload) {
        if (!running) {
            return;
        }
        int len = payload.length;

        for (Iterator<DataOutputStream> it = clients.iterator(); it.hasNext();) {
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

        // ★ 追加: accept を強制的に解除するため、listen ソケットを閉じる
        ServerSocket ss = serverSocket;
        serverSocket = null;
        if (ss != null) {
            try {
                ss.close(); // これで accept() が IOException を投げてスレッドが抜ける
            } catch (IOException ignore) {
            }
        }

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
