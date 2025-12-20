package v2x.vehicle.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.net.Topics;
import v2x.vehicle.ros.timeline.TimelineFiles;
import v2x.vehicle.ros.timeline.TimelineLog;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static v2x.vehicle.mqtt.MqttRelayMain.Env.*;

/**
 * rosjava の PublisherTimelineLoop 相当（ただし ROS なし / MQTT に publish）
 *
 * 1フレーム: - timeline json 読む - region リスト取得 - 各 region の PCD を
 * readRawPcdWithName(region, frame) で読む - Topics.data(region) に
 * publish（点群もブローカ経由）
 */
public final class MqttPublisherTimelineLoop {

    private final MqttClient client;
    private final AppConfig cfg;
    private final int qos;

    private final String timelineDir;
    private final long stepMs;
    private final boolean loop;
    private final long syncUnixSec;

    private final boolean enabled;

    public MqttPublisherTimelineLoop(MqttClient client, AppConfig cfg, int qos) {
        this.client = client;
        this.cfg = cfg;
        this.qos = qos;

        this.timelineDir = env("REGION_TIMELINE_DIR", "");
        this.stepMs = envLong("REGION_TIMELINE_STEP_MS", 100L);
        this.loop = envBool01("REGION_TIMELINE_LOOP", false);
        this.syncUnixSec = envLong("REGION_TIMELINE_SYNC_UNIX_SEC", 0L); // 任意

        this.enabled = (timelineDir != null && !timelineDir.isBlank());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void runForever() {
        if (!enabled) {
            return;
        }

        try {
            List<Path> frames = TimelineFiles.listTimelineFiles(Path.of(timelineDir));
            if (frames.isEmpty()) {
                TimelineLog.error("MQTT-PUB-TL", "no timeline json under " + timelineDir);
                return;
            }

            // 既存設計に合わせて datasetRoot=timelineDir（regionId サブディレクトリがある想定）
            DatasetPointCloudSource ds = new DatasetPointCloudSource(
                    timelineDir,
                    cfg.datasetGlob,
                    cfg.datasetLoop
            );

            TimelineLog.logf("MQTT-PUB-TL", "started timelineDir=%s frames=%d stepMs=%d loop=%s qos=%d",
                    timelineDir, frames.size(), stepMs, loop, qos);

            int frameIndex = 0;

            while (true) {
                waitUntilSync(syncUnixSec, stepMs);

                if (!loop && frameIndex >= frames.size()) {
                    TimelineLog.logf("MQTT-PUB-TL", "timeline finished frames=%d", frames.size());
                    return;
                }

                int idx = frameIndex % frames.size();
                Path framePath = frames.get(idx);

                TimelineLog.logf("MQTT-PUB-TL", "reading timeline frame=%d file=%s", idx, framePath.getFileName());

                List<String> regions = TimelineFiles.readRegionsFromJson(framePath);
                LinkedHashSet<String> current = new LinkedHashSet<>(regions);

                long t0 = System.nanoTime();
                int published = 0;

                for (String regionId : current) {
                    try {
                        DatasetPointCloudSource.RawPcd raw = ds.readRawPcdWithName(regionId, idx);
                        if (raw == null || raw.data == null) {
                            continue;
                        }

                        byte[] payload = packNameAndBodyLE(raw.fileName, raw.data);
                        String topic = Topics.data(regionId);

                        MqttMessage m = new MqttMessage(payload);
                        m.setQos(qos);
                        client.publish(topic, m);
                        published++;

                        TimelineLog.logf("MQTT-PUB", "published RAW frame=%d region=%s file=%s bytes=%d topic=%s",
                                idx, regionId, raw.fileName, payload.length, topic);
                    } catch (Exception e) {
                        TimelineLog.error("MQTT-PUB", "publish failed frame=" + idx + " region=" + regionId, e);
                    }
                }

                long dtMs = (System.nanoTime() - t0) / 1_000_000L;
                TimelineLog.logf("MQTT-PUB-TL", "frame done frame=%d regions=%d published=%d workMs=%d",
                        idx, current.size(), published, dtMs);

                // 「処理がstepMsを超える」場合は sleep が短く/0になり、結果として遅く見える（想定どおり）
                TimeUnit.MILLISECONDS.sleep(stepMs);
                frameIndex++;
            }

        } catch (Exception e) {
            TimelineLog.error("MQTT-PUB-TL", "publisher loop failed", e);
        }
    }

    private static void waitUntilSync(long syncUnixSec, long stepMs) throws InterruptedException {
        if (syncUnixSec <= 0L) {
            return;
        }
        long nowSec = System.currentTimeMillis() / 1000L;
        if (nowSec < syncUnixSec) {
            long remainMs = (syncUnixSec - nowSec) * 1000L;
            TimeUnit.MILLISECONDS.sleep(Math.min(stepMs, remainMs));
        }
    }

    // payload: [4 nameLen LE][name UTF-8][PCD bytes]
    private static byte[] packNameAndBodyLE(String fileName, byte[] body) {
        byte[] nameBytes = fileName.getBytes(StandardCharsets.UTF_8);

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
}
