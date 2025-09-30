package org.example.v2x.coordinator;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

/**
 * （オプション）UDP中継の簡易サーバ。実験では車両間直送で良いが、 同一ホスト内のプロセス間疎通確認にも使える。
 */
public class UdpRelayServer implements Runnable {

    private final int port;

    public UdpRelayServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        byte[] buf = new byte[64 * 1024];
        try (DatagramSocket socket = new DatagramSocket(port)) {
            while (true) {
                DatagramPacket p = new DatagramPacket(buf, buf.length);
                socket.receive(p);
// 受け取った送信者にそのままエコー（デモ用）
                InetAddress addr = p.getAddress();
                int srcPort = p.getPort();
                socket.send(new DatagramPacket(p.getData(), p.getLength(), addr, srcPort));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
