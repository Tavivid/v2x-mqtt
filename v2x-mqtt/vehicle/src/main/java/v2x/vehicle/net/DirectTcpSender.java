package v2x.vehicle.net;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * 1対1の直送TCP送信。 フレーム形式: int32 payloadLen (big endian; DataOutputStream)
 * byte[payloadLen] payload
 */
public final class DirectTcpSender implements AutoCloseable {

    private final String host;
    private final int port;

    private Socket socket;
    private DataOutputStream out;

    public DirectTcpSender(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public synchronized void connectIfNeeded() throws IOException {
        if (socket != null && socket.isConnected() && !socket.isClosed()) {
            return;
        }

        close();
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 3000);
        out = new DataOutputStream(socket.getOutputStream());
        System.out.println("[DirectTcpSender] connected to " + host + ":" + port);
    }

    public synchronized void send(byte[] payload) throws IOException {
        connectIfNeeded();
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    @Override
    public synchronized void close() {
        if (out != null) {
            try {
                out.close();
            } catch (IOException ignore) {
            }
            out = null;
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignore) {
            }
            socket = null;
        }
    }
}
