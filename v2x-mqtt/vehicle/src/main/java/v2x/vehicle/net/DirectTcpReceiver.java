package v2x.vehicle.net;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Consumer;

/**
 * 1対1の直受けTCP受信。 受信形式: int32 payloadLen payload bytes
 */
public final class DirectTcpReceiver implements AutoCloseable {

    private final int listenPort;
    private final Consumer<byte[]> callback;

    private volatile boolean running = false;
    private Thread acceptThread;
    private volatile ServerSocket serverSocket;

    public DirectTcpReceiver(int listenPort, Consumer<byte[]> callback) {
        this.listenPort = listenPort;
        this.callback = callback;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;

        acceptThread = new Thread(() -> {
            try (ServerSocket ss = new ServerSocket(listenPort)) {
                serverSocket = ss;
                System.out.println("[DirectTcpReceiver] listen on " + listenPort);

                while (running) {
                    try (Socket sock = ss.accept(); DataInputStream in = new DataInputStream(sock.getInputStream())) {

                        System.out.println("[DirectTcpReceiver] client connected from " + sock.getRemoteSocketAddress());

                        while (running) {
                            int len;
                            try {
                                len = in.readInt();
                            } catch (EOFException eof) {
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
                        if (running) {
                            System.err.println("[DirectTcpReceiver] client error: " + e.getMessage());
                        }
                    }
                }
            } catch (IOException e) {
                if (running) {
                    System.err.println("[DirectTcpReceiver] listen failed: " + e.getMessage());
                }
            } finally {
                serverSocket = null;
            }
        }, "DirectTcpReceiver-" + listenPort);

        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    @Override
    public synchronized void close() {
        running = false;
        ServerSocket ss = serverSocket;
        serverSocket = null;
        if (ss != null) {
            try {
                ss.close();
            } catch (IOException ignore) {
            }
        }
        if (acceptThread != null) {
            acceptThread.interrupt();
        }
    }
}
