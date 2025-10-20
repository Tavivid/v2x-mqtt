package org.example.v2x.vehicle.net;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UdpSender {

    private final int port;

    public UdpSender(int port) {
        this.port = port;
    }

    public void send(byte[] data, String host) throws Exception {
        try (DatagramSocket s = new DatagramSocket()) {
            DatagramPacket p = new DatagramPacket(data, data.length, InetAddress.getByName(host), port);
            s.send(p);
        }
    }
}
