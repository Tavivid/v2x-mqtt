package v2x.vehicle.net;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * ROS1 の TCPROS Subscriber に相当する簡易実装。
 *
 * - Master に定期的に LOOKUP_PUBS して Publisher 一覧を取得
 * - 新しく見つかった Publisher に TCP 接続し、長さ付きフレーム (int length + payload) を読み続ける
 * - publisher が後から起動しても、次の lookup タイミングで自動的に接続する
 */
public class RosSubscriber implements AutoCloseable {

    private final MasterClient master;
    private final String topic;
    private final Consumer<byte[]> callback;

    // publisher ごとの受信スレッド
    private final CopyOnWriteArrayList<Thread> receiverThreads = new CopyOnWriteArrayList<>();
    // すでに接続開始した Endpoint の集合（重複接続防止）
    private final CopyOnWriteArraySet<MasterClient.Endpoint> connectedEndpoints = new CopyOnWriteArraySet<>();

    // publisher リストを定期的に問い合わせるスレッド
    private volatile Thread resolverThread;

    private volatile boolean running = false;

    public RosSubscriber(MasterClient master, String topic, Consumer<byte[]> callback) {
        this.master = master;
        this.topic = topic;
        this.callback = callback;
    }

    /**
     * Subscriber を開始する。
     *
     * - 直ちに Master に対して lookup ループ用の resolverThread を起動する
     * - resolverThread は 1 秒ごとに LOOKUP_PUBS を実行し、新規 Endpoint にだけ startReceiver() を呼ぶ
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;

        Thread t = new Thread(() -> {
            System.out.println("[RosSubscriber] resolver started for topic=" + topic);
            while (running) {
                try {
                    List<MasterClient.Endpoint> eps = master.lookupPublishers(topic);
                    if (eps.isEmpty()) {
                        System.out.println("[RosSubscriber] no publishers for topic=" + topic);
                    }
                    for (MasterClient.Endpoint ep : eps) {
                        // まだ接続開始していない endpoint だけ新しく Receiver スレッドを起動
                        if (!connectedEndpoints.contains(ep)) {
                            connectedEndpoints.add(ep);
                            startReceiver(ep);
                        }
                    }
                } catch (IOException e) {
                    System.err.println("[RosSubscriber] lookupPublishers error for topic=" + topic + ": " + e.getMessage());
                }

                try {
                    Thread.sleep(1000); // 1 秒おきに再 lookup
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            System.out.println("[RosSubscriber] resolver stopped for topic=" + topic);
        }, "RosSubscriber-resolver-" + topic);

        t.setDaemon(true);
        t.start();
        this.resolverThread = t;
    }

    /**
     * 指定 Endpoint に対する受信スレッドを起動する。
     * 接続が切れた場合は 1 秒待って自動再接続を試みる。
     */
    private void startReceiver(MasterClient.Endpoint ep) {
        Thread t = new Thread(() -> {
            while (running) {
                try (Socket sock = new Socket(ep.host, ep.port);
                     DataInputStream in = new DataInputStream(sock.getInputStream())) {

                    System.out.println("[RosSubscriber] connected to publisher " + ep + " for topic=" + topic);

                    while (running) {
                        int len;
                        try {
                            len = in.readInt();
                        } catch (EOFException e) {
                            // publisher 側がソケットを閉じた
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
                    if (!running) {
                        break;
                    }
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

        // resolver を止める
        Thread rt = resolverThread;
        if (rt != null) {
            rt.interrupt();
        }

        // 受信スレッドを止める
        for (Thread t : receiverThreads) {
            t.interrupt();
        }
        receiverThreads.clear();
        connectedEndpoints.clear();
    }
}
