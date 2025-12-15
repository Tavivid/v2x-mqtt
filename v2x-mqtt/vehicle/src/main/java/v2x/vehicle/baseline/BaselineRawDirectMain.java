package v2x.vehicle.baseline;

import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.FlatDatasetPointCloudSource;
import v2x.vehicle.net.DirectTcpReceiver;
import v2x.vehicle.net.DirectTcpSender;
import v2x.vehicle.ros.timeline.TimelineFiles;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * baseline_raw_direct (Pub/Subなし, 領域分割なし, roscore不要)
 *
 * 送信: datasetDir/%06d.pcd を読み、TCPで直送
 * 受信: TCPで受けてファイル保存
 *
 * 環境変数:
 *   BASELINE_ROLE=send|recv|both   (default: both)
 *   BASELINE_DST_HOST             (send時に必須)
 *   BASELINE_DST_PORT             (default: 60000)
 *   BASELINE_LISTEN_PORT          (default: 60000)
 *   BASELINE_DATASET_DIR          (default: REGION_TIMELINE_DIR, その次に DATASET_PATH, 最後に ./dataset)
 *   BASELINE_RECV_DIR             (default: AppConfig.transferRecvDir)
 *   BASELINE_STEP_MS              (default: REGION_TIMELINE_STEP_MS or 100)
 *   BASELINE_LOOP=0|1             (default: 0)
 *   BASELINE_TIMELINE_DIR         (default: REGION_TIMELINE_DIR) ※jsonがあるなら終端判定に使う
 */
public final class BaselineRawDirectMain {

  public static void main(String[] args) throws Exception {
    AppConfig cfg = AppConfig.load();

    String role = env("BASELINE_ROLE", "both").trim().toLowerCase();
    boolean doSend = role.equals("send") || role.equals("both");
    boolean doRecv = role.equals("recv") || role.equals("both");

    int dstPort = envInt("BASELINE_DST_PORT", 60000);
    int listenPort = envInt("BASELINE_LISTEN_PORT", 60000);

    // datasetDir: BASELINE_DATASET_DIR > REGION_TIMELINE_DIR > DATASET_PATH > ./dataset
    String datasetDir = env("BASELINE_DATASET_DIR", "");
    if (datasetDir.isBlank()) datasetDir = env("REGION_TIMELINE_DIR", "");
    if (datasetDir.isBlank()) datasetDir = env("DATASET_PATH", cfg.datasetPath);
    if (datasetDir.isBlank()) datasetDir = "./dataset";

    String recvDir = env("BASELINE_RECV_DIR", cfg.transferRecvDir);

    long stepMs = envLong("BASELINE_STEP_MS", envLong("REGION_TIMELINE_STEP_MS", 100L));
    boolean loop = "1".equals(env("BASELINE_LOOP", "0"));

    String timelineDirStr = env("BASELINE_TIMELINE_DIR", env("REGION_TIMELINE_DIR", ""));
    File timelineDir = timelineDirStr.isBlank() ? null : new File(timelineDirStr);

    System.out.println("[BaselineRawDirectMain] role=" + role);
    System.out.println("[BaselineRawDirectMain] datasetDir=" + datasetDir);
    System.out.println("[BaselineRawDirectMain] recvDir=" + recvDir);
    System.out.println("[BaselineRawDirectMain] stepMs=" + stepMs + " loop=" + loop);
    System.out.println("[BaselineRawDirectMain] listenPort=" + listenPort + " dstPort=" + dstPort);

    // ===== Receiver =====
    DirectTcpReceiver receiver = null;
    if (doRecv) {
      receiver = new DirectTcpReceiver(listenPort, payload -> {
        try {
          // payload: [4byte nameLen LE][name utf8][pcd bytes]
          if (payload.length < 12) throw new IllegalStateException("payload too short");
          long recvNano = System.nanoTime();
          long sendNano = getLongLE(payload, 0);
          int nameLen = getIntLE(payload, 8);
          if (nameLen < 0 || 12 + nameLen > payload.length) {
            throw new IllegalStateException("invalid nameLen=" + nameLen + " payloadLen=" + payload.length);
          }
          String fileName = new String(payload, 12, nameLen, StandardCharsets.UTF_8);
          int pcdOff = 12 + nameLen;
          int pcdLen = payload.length - pcdOff;

          File outBase = new File(recvDir);
          outBase.mkdirs();
          File out = new File(outBase, fileName);
          Files.write(out.toPath(), slice(payload, pcdOff, pcdLen));

          long latencyNs = recvNano - sendNano;
          double latencyMs = latencyNs / 1_000_000.0;

          System.out.println(String.format(
              "[BASE-SUB] saved PCD: %s bytes=%d latency_ns=%d latency_ms=%.3f sendNano=%d recvNano=%d",
              out.getAbsolutePath(), pcdLen, latencyNs, latencyMs, sendNano, recvNano
          ));
        } catch (Exception e) {
          System.err.println("[BASE-SUB] failed: " + e.getMessage());
        }
      });
      receiver.start();
    }

    // ===== Sender =====
    if (doSend) {
      String dstHost = env("BASELINE_DST_HOST", "").trim();
      if (dstHost.isBlank()) {
        System.err.println("[BASE-PUB] BASELINE_DST_HOST is empty. (send role requires it)");
      } else {
        FlatDatasetPointCloudSource src = new FlatDatasetPointCloudSource(datasetDir);
        DirectTcpSender sender = new DirectTcpSender(dstHost, dstPort);

        // jsonタイムラインがある場合は「何フレームで終わるか」の目安に使う（region内容は無視）
        List<Path> timelineFrames = List.of();
        if (timelineDir != null && timelineDir.isDirectory()) {
          timelineFrames = TimelineFiles.listTimelineFiles(timelineDir.toPath());
        }
        final int timelineSize = timelineFrames.size();

        int frameIndex = 0;
        while (true) {
          int idx = (timelineSize > 0) ? (frameIndex % timelineSize) : frameIndex;

          FlatDatasetPointCloudSource.RawPcd raw = src.readRawPcdWithName(idx);

          if (raw == null) {
            // 終端判定：タイムラインありなら「1周したら終わり」、なければ「ファイル欠損で終了」
            if (!loop) {
              System.out.println("[BASE-PUB] finished. (no file) frame=" + idx);
              break;
            }
            // loop時は少し待ってリトライ（datasetが遅れて生成されるケースにも耐える）
            TimeUnit.MILLISECONDS.sleep(stepMs);
            frameIndex++;
            continue;
          }

          long sendNano = System.nanoTime();
          byte[] payload = packNanoNameAndBodyLE(sendNano, raw.fileName, raw.data);
          sender.send(payload);

          System.out.println("[BASE-PUB] sent RAW frame=" + idx
              + " file=" + raw.fileName
              + " bytes=" + payload.length
              + " sendNano=" + sendNano
              + " dst=" + dstHost + ":" + dstPort);

          TimeUnit.MILLISECONDS.sleep(stepMs);

          frameIndex++;
          if (!loop && timelineSize > 0 && frameIndex >= timelineSize) {
            System.out.println("[BASE-PUB] finished. (timeline end) frames=" + timelineSize);
            break;
          }
        }

        sender.close();
      }
    }

    // recvのみの場合は待機し続ける
    if (doRecv && !doSend) {
      System.out.println("[BASE-SUB] waiting forever...");
      while (true) Thread.sleep(60_000);
    }

    if (receiver != null) receiver.close();
  }

  // ===== util =====
  private static String env(String k, String def) {
    String v = System.getenv(k);
    return (v == null) ? def : v;
  }

  private static int envInt(String k, int def) {
    String v = System.getenv(k);
    if (v == null || v.isBlank()) return def;
    try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return def; }
  }

  private static long envLong(String k, long def) {
    String v = System.getenv(k);
    if (v == null || v.isBlank()) return def;
    try { return Long.parseLong(v.trim()); } catch (NumberFormatException e) { return def; }
  }

  private static int getIntLE(byte[] a, int off) {
    return (a[off] & 0xff)
        | ((a[off + 1] & 0xff) << 8)
        | ((a[off + 2] & 0xff) << 16)
        | ((a[off + 3] & 0xff) << 24);
  }

  private static long getLongLE(byte[] a, int off) {
   return ((long) (a[off] & 0xff))
        | ((long) (a[off + 1] & 0xff) << 8)
        | ((long) (a[off + 2] & 0xff) << 16)
        | ((long) (a[off + 3] & 0xff) << 24)
        | ((long) (a[off + 4] & 0xff) << 32)
        | ((long) (a[off + 5] & 0xff) << 40)
        | ((long) (a[off + 6] & 0xff) << 48)
        | ((long) (a[off + 7] & 0xff) << 56);
  }

  private static byte[] slice(byte[] a, int off, int len) {
    byte[] b = new byte[len];
    System.arraycopy(a, off, b, 0, len);
    return b;
  }

  private static byte[] packNameAndBodyLE(String fileName, byte[] body) {
    byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);

    // [4byte nameLen LE][name][body]
    int total = 4 + nameBytes.length + body.length;
    byte[] out = new byte[total];

    int n = nameBytes.length;
    out[0] = (byte) (n & 0xff);
    out[1] = (byte) ((n >>> 8) & 0xff);
    out[2] = (byte) ((n >>> 16) & 0xff);
    out[3] = (byte) ((n >>> 24) & 0xff);

    System.arraycopy(nameBytes, 0, out, 4, nameBytes.length);
    System.arraycopy(body, 0, out, 4 + nameBytes.length, body.length);
    return out;
  }

  private static byte[] packNanoNameAndBodyLE(long sendNano, String fileName, byte[] body) {
    byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);

    // [8byte sendNano LE][4byte nameLen LE][name][body]
    int total = 8 + 4 + nameBytes.length + body.length;
    byte[] out = new byte[total];

    // sendNano
    out[0] = (byte) (sendNano);
    out[1] = (byte) (sendNano >>> 8);
    out[2] = (byte) (sendNano >>> 16);
    out[3] = (byte) (sendNano >>> 24);
    out[4] = (byte) (sendNano >>> 32);
    out[5] = (byte) (sendNano >>> 40);
    out[6] = (byte) (sendNano >>> 48);
    out[7] = (byte) (sendNano >>> 56);

    // nameLen
    int n = nameBytes.length;
    out[8]  = (byte) (n);
    out[9]  = (byte) (n >>> 8);
    out[10] = (byte) (n >>> 16);
    out[11] = (byte) (n >>> 24);

    System.arraycopy(nameBytes, 0, out, 12, nameBytes.length);
    System.arraycopy(body, 0, out, 12 + nameBytes.length, body.length);
    return out;
  }

}
