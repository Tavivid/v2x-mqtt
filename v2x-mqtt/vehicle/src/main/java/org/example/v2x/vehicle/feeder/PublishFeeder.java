package org.example.v2x.vehicle.feeder;

import org.eclipse.paho.client.mqttv3.*;
import org.example.v2x.vehicle.request.RequestPublisher;
import org.example.v2x.vehicle.sub.DynamicSubscriptionManager;

import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CSV を読み、指定タイミングで /data に繋げる簡易フィーダ。
 *
 * フォーマット:
 *   # at_ms,region_id,ip,port
 *   0,cell-12,127.0.0.1,51235
 *   1000,cell-13,127.0.0.1,51235
 *
 * - at_ms: フィーダ開始からの相対ミリ秒
 * - region: 発行リージョン
 */
public class PublishFeeder {
  private final File csv;

  public PublishFeeder(File csv) {
    this.csv = csv;
  }

  static final class Row {
    final long atMs;
    final String region;
    Row(long atMs, String region) {
      this.atMs = atMs; this.region = region;
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
        if (tk.length < 2)
          throw new IllegalArgumentException("SUB CSV format error at line " + lineno + " (expect: at_ms,region_id)");
        long at = Long.parseLong(tk[0].trim());
        String region = tk[1].trim();
        rows.add(new Row(at, region));
      }
    }
    return rows.stream().sorted(Comparator.comparingLong(r -> r.atMs)).collect(Collectors.toList());
  }
  
  public void run(DynamicSubscriptionManager dynSub) {
    try {
      List<Row> rows = load();
      if (rows.isEmpty()) { 
        System.out.println("[FEED-SUB] no rows.");
        return;
      }
      long start = System.currentTimeMillis();

      for (Row r : rows) {
        long due = start + r.atMs;
        long now = System.currentTimeMillis();
        if (due > now) TimeUnit.MILLISECONDS.sleep(due - now);
        dynSub.touch(r.region);
        System.out.println("[FEED-SUB] touch region=" + r.region);
      }
      System.out.println("[FEED-SUB] sequence done");
    } catch (Exception e) {
      System.err.println("[FEED-SUB] error: " + e);
    }
  }
}
