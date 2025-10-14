package org.example.v2x.vehicle.net;

import org.example.v2x.common.config.AppConfig;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** Vehicleプロセス内でUDP受信をバックグラウンド起動するためのサービス */
public final class UdpSinkService implements AutoCloseable, Runnable {
  private final Path outDir;
  private final int port;
  private volatile boolean running = true;
  private Thread thread;

  public UdpSinkService(AppConfig cfg) throws Exception {
    this.outDir = Paths.get(cfg.transferRecvDir).toAbsolutePath().normalize();
    Files.createDirectories(this.outDir);
    this.port   = cfg.udpRecvPort;
  }

  /** 非同期開始 */
  public void start() {
    if (thread != null) return;
    thread = new Thread(this, "udp-sink");
    thread.setDaemon(true);
    thread.start();
    System.out.println("[UDP-SINK] started in background on " + port + ", dir=" + outDir);
  }

  @Override public void run() {
    byte[] buf = new byte[64 * 1024];
    try (DatagramSocket sock = new DatagramSocket(port)) {
      while (running) {
        DatagramPacket p = new DatagramPacket(buf, buf.length);
        sock.receive(p);
        byte[] data = java.util.Arrays.copyOfRange(p.getData(), p.getOffset(), p.getOffset() + p.getLength());
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"));
        Path out = outDir.resolve("recv-" + ts + "-from-" + p.getAddress().getHostAddress() + "-" + p.getPort() + ".bin");
        Files.write(out, data);
        System.out.println("[UDP-SINK] saved " + out + " (" + data.length + " bytes)");
      }
    } catch (Exception e) {
      if (running) System.err.println("[UDP-SINK] error: " + e);
    }
  }

  @Override public void close() {
    running = false;
    if (thread != null) thread.interrupt();
  }
}
