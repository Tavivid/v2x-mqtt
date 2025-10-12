package org.example.v2x.vehicle.feeder;

import org.example.v2x.vehicle.request.RequestPublisher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV を読み、指定タイミングで /request を投げる簡易フィーダ。
 *
 * フォーマット:
 *   # at_ms,region_id,ip,port
 *   0,cell-12,127.0.0.1,51235
 *   1000,cell-13,127.0.0.1,51235
 *
 * - at_ms: フィーダ開始からの相対ミリ秒
 * - region_id: 要求リージョン
 * - ip/port: Requester(=このVehicle)のUDP受信先
 */
public class RegionRequestFeeder implements Runnable {
  private final File csv;
  private final RequestPublisher publisher;
  private final boolean loop;

  public RegionRequestFeeder(File csv, RequestPublisher publisher, boolean loop) {
    this.csv = csv;
    this.publisher = publisher;
    this.loop = loop;
  }

  static final class Row {
    final long atMs;
    final String region;
    final InetSocketAddress rx;
    Row(long atMs, String region, InetSocketAddress rx) {
      this.atMs = atMs; this.region = region; this.rx = rx;
    }
  }

  private List<Row> load() throws Exception {
    List<Row> rows = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
      String line;
      long lineno = 0;
      while ((line = br.readLine()) != null) {
        lineno++;
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) continue;
        String[] tk = line.split(",", -1);
        if (tk.length < 4) throw new IllegalArgumentException("CSV format: at_ms,region_id,ip,port at line " + lineno);
        long at = Long.parseLong(tk[0].trim());
        String region = tk[1].trim();
        String ip = tk[2].trim();
        int port = Integer.parseInt(tk[3].trim());
        rows.add(new Row(at, region, new InetSocketAddress(ip, port)));
      }
    }
    rows.sort((a,b) -> Long.compare(a.atMs, b.atMs));
    return rows;
  }

  @Override
  public void run() {
    try {
      List<Row> rows = load();
      if (rows.isEmpty()) {
        System.out.println("[FEED] no rows. nothing to do.");
        return;
      }
      do {
        long start = System.currentTimeMillis();
        for (Row r : rows) {
          long due = start + r.atMs;
          long now = System.currentTimeMillis();
          if (due > now) Thread.sleep(due - now);
          publisher.publish(r.region, r.rx);
        }
        System.out.println("[FEED] sequence done" + (loop ? " (looping)":""));
      } while (loop);
    } catch (Exception e) {
      System.err.println("[FEED] error: " + e);
    }
  }
}
