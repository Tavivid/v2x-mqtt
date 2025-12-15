package v2x.vehicle.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import v2x.vehicle.net.Topics;
import v2x.vehicle.ros.timeline.TimelineFiles;
import v2x.vehicle.ros.timeline.TimelineLog;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static v2x.vehicle.mqtt.MqttRelayMain.Env.*;

/**
 * rosjava の SubscriberTimelineLoop 相当（ただし ROS なし / MQTT を subscribe）
 *
 * 1フレーム:
 *  - timeline json 読む
 *  - region リスト取得
 *  - active set を更新（subscribe/unsubscribe）
 *
 * 受信:
 *  - Topics.data(region) から payload を受け取る
 *  - payload をパースしてファイル保存
 *
 * ★重要:
 *  - MQTT callback 内で Files.write() すると遅くなりやすいので、
 *    queue に積んで保存スレッドで書く（rosjava側の設計に寄せつつ詰まりを避ける）
 */
public final class MqttSubscriberTimelineLoop {

  private final MqttClient client;
  private final int qos;

  private final String timelineDir;
  private final long stepMs;
  private final boolean loop;
  private final long syncUnixSec;

  private final String recvDir;
  private final int saveQueueCap;
  private final int saveThreads;

  private final boolean enabled;

  private final BlockingQueue<SaveJob> saveQueue;
  private final ExecutorService savePool;

  private final Map<String, Boolean> active = new HashMap<>();

  public MqttSubscriberTimelineLoop(MqttClient client, int qos) {
    this.client = client;
    this.qos = qos;

    this.timelineDir = env("SUB_TIMELINE_DIR", "");
    this.stepMs = envLong("SUB_TIMELINE_STEP_MS", 100L);
    this.loop = envBool01("SUB_TIMELINE_LOOP", false);
    this.syncUnixSec = envLong("SUB_TIMELINE_SYNC_UNIX_SEC", 0L); // 任意

    this.recvDir = env("MQTT_RELAY_RECV_DIR", "/home/tavivid/v2x-pcd/received_raw_mqttrelay");
    this.saveQueueCap = envInt("MQTT_SAVE_QUEUE", 4096);
    this.saveThreads = envInt("MQTT_SAVE_THREADS", 1);

    this.enabled = (timelineDir != null && !timelineDir.isBlank());

    this.saveQueue = new ArrayBlockingQueue<>(saveQueueCap);
    this.savePool = Executors.newFixedThreadPool(saveThreads, r -> {
      Thread t = new Thread(r, "MqttRelaySaveWorker");
      t.setDaemon(true);
      return t;
    });

    for (int i = 0; i < saveThreads; i++) {
      savePool.submit(this::saveWorkerLoop);
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void runForever() {
    if (!enabled) return;

    try {
      List<Path> frames = TimelineFiles.listTimelineFiles(Path.of(timelineDir));
      if (frames.isEmpty()) {
        TimelineLog.error("MQTT-SUB-TL", "no timeline json under " + timelineDir);
        return;
      }

      TimelineLog.logf("MQTT-SUB-TL", "started timelineDir=%s frames=%d stepMs=%d loop=%s qos=%d recvDir=%s",
          timelineDir, frames.size(), stepMs, loop, qos, recvDir);

      int frameIndex = 0;

      while (true) {
        waitUntilSync(syncUnixSec, stepMs);

        if (!loop && frameIndex >= frames.size()) {
          TimelineLog.logf("MQTT-SUB-TL", "timeline finished frames=%d", frames.size());
          return;
        }

        int idx = frameIndex % frames.size();
        Path framePath = frames.get(idx);

        TimelineLog.logf("MQTT-SUB-TL", "reading timeline frame=%d file=%s", idx, framePath.getFileName());

        List<String> regions = TimelineFiles.readRegionsFromJson(framePath);
        LinkedHashSet<String> current = new LinkedHashSet<>(regions);

        // unsubscribe
        Set<String> toStop = new HashSet<>(active.keySet());
        toStop.removeAll(current);
        for (String r : toStop) {
          String topic = Topics.data(r);
          try { client.unsubscribe(topic); } catch (Exception ignore) {}
          active.remove(r);
          TimelineLog.logf("MQTT-SUB-TL", "unsubscribe region=%s topic=%s", r, topic);
        }

        // subscribe
        Set<String> toStart = new HashSet<>(current);
        toStart.removeAll(active.keySet());
        for (String r : toStart) {
          String topic = Topics.data(r);

          client.subscribe(topic, qos, (t, msg) -> {
            try {
              byte[] payload = msg.getPayload();
              if (payload == null || payload.length < 4) return;

              int nameLen = getIntLE(payload, 0);
              if (nameLen < 0 || 4 + nameLen > payload.length) return;

              String fileName = new String(payload, 4, nameLen, StandardCharsets.UTF_8);
              int pcdOff = 4 + nameLen;
              int pcdLen = payload.length - pcdOff;

              byte[] pcd = new byte[pcdLen];
              System.arraycopy(payload, pcdOff, pcd, 0, pcdLen);

              // callback内では保存しない。キューへ。
              if (!saveQueue.offer(new SaveJob(r, fileName, pcd))) {
                TimelineLog.error("MQTT-SUB", "save queue full. drop region=" + r + " file=" + fileName);
              }
            } catch (Exception e) {
              TimelineLog.error("MQTT-SUB", "handle incoming failed topic=" + t, e);
            }
          });

          active.put(r, true);
          TimelineLog.logf("MQTT-SUB-TL", "subscribe region=%s topic=%s qos=%d", r, topic, qos);
        }

        TimeUnit.MILLISECONDS.sleep(stepMs);
        frameIndex++;
      }

    } catch (Exception e) {
      TimelineLog.error("MQTT-SUB-TL", "subscriber loop failed", e);
    } finally {
      try { savePool.shutdownNow(); } catch (Exception ignore) {}
    }
  }

  private void saveWorkerLoop() {
    while (true) {
      try {
        SaveJob job = saveQueue.take();

        File base = new File(recvDir);
        File outDir = new File(base, job.regionId);
        outDir.mkdirs();

        File out = new File(outDir, job.fileName);
        Files.write(out.toPath(), job.pcdBytes);

        TimelineLog.logf("MQTT-SUB", "saved PCD: %s bytes=%d", out.getAbsolutePath(), job.pcdBytes.length);
      } catch (InterruptedException ie) {
        return;
      } catch (Exception e) {
        TimelineLog.error("MQTT-SUB", "save failed", e);
      }
    }
  }

  private static void waitUntilSync(long syncUnixSec, long stepMs) throws InterruptedException {
    if (syncUnixSec <= 0L) return;
    long nowSec = System.currentTimeMillis() / 1000L;
    if (nowSec < syncUnixSec) {
      long remainMs = (syncUnixSec - nowSec) * 1000L;
      TimeUnit.MILLISECONDS.sleep(Math.min(stepMs, remainMs));
    }
  }

  private static int getIntLE(byte[] a, int off) {
    return (a[off] & 0xff)
        | ((a[off + 1] & 0xff) << 8)
        | ((a[off + 2] & 0xff) << 16)
        | ((a[off + 3] & 0xff) << 24);
  }

  private static final class SaveJob {
    final String regionId;
    final String fileName;
    final byte[] pcdBytes;
    SaveJob(String regionId, String fileName, byte[] pcdBytes) {
      this.regionId = regionId;
      this.fileName = fileName;
      this.pcdBytes = pcdBytes;
    }
  }
}
