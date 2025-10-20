package v2x.vehicle.net;

import v2x.vehicle.config.AppConfig;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class UdpSink {
  public static void main(String[] args) throws Exception {
    AppConfig cfg = AppConfig.load();
    Path outDir = Paths.get(cfg.transferRecvDir).toAbsolutePath().normalize();
    Files.createDirectories(outDir);
    int port = cfg.udpRecvPort;

    try (DatagramSocket sock = new DatagramSocket(port)) {
      System.out.println("[UDP-SINK] listening on " + port + ", saving into " + outDir);
      byte[] buf = new byte[64 * 1024];
      while (true) {
        DatagramPacket p = new DatagramPacket(buf, buf.length);
        sock.receive(p);
        byte[] data = java.util.Arrays.copyOfRange(p.getData(), p.getOffset(), p.getOffset() + p.getLength());
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss-SSS"));
        Path out = outDir.resolve("recv-" + ts + "-from-" + p.getAddress().getHostAddress() + "-" + p.getPort() + ".bin");
        Files.write(out, data);
        System.out.println("[UDP-SINK] saved " + out + " (" + data.length + " bytes)");
      }
    }
  }
}
