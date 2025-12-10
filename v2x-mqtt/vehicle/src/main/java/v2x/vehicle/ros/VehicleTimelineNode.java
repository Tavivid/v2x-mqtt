package v2x.vehicle.ros;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.ros.concurrent.CancellableLoop;
import org.ros.namespace.GraphName;
import org.ros.node.AbstractNodeMain;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Publisher;
import org.ros.node.topic.Subscriber;
import org.ros.message.MessageListener;
import std_msgs.ByteMultiArray;
import v2x.vehicle.config.AppConfig;
import v2x.vehicle.datasource.DatasetPointCloudSource;
import v2x.vehicle.model.Point3D;
import v2x.vehicle.model.PointCloudChunk;
import v2x.vehicle.net.Topics;
import v2x.vehicle.util.Jsons;
import v2x.vehicle.util.PointCloudSerializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * JSON タイムラインに従って複数領域を pub/sub する rosjava ノード。
 *
 * - Publisher 側:
 *   REGION_TIMELINE_DIR   : フレームごとの JSON が並ぶディレクトリ
 *   REGION_TIMELINE_STEP_MS: フレーム間隔 (ms) デフォルト 100
 *   REGION_TIMELINE_LOOP  : "1" なら最後まで行ったら先頭に戻る
 *   JSON 形式:
 *     ["cell-0a3c", "cell-0a3d"]
 *     または { "regions": ["cell-0a3c", "cell-0a3d"] }
 *
 *   Dataset は RegionTimelineFeeder と同様に
 *   datasetRoot = REGION_TIMELINE_DIR を渡した DatasetPointCloudSource を使う。
 *
 * - Subscriber 側:
 *   SUB_TIMELINE_DIR, SUB_TIMELINE_STEP_MS, SUB_TIMELINE_LOOP
 *   の意味は RegionTimelineSubscriberFeeder と同じ。
 *
 * メッセージ型は std_msgs/ByteMultiArray を使い、
 * data 部分に PointCloudSerializer のバイト列をそのまま入れる。
 */
public class VehicleTimelineNode extends AbstractNodeMain {

    private final AppConfig cfg;
    private final String vehicleId;

    // Publisher 用タイムライン設定
    private final File pubTimelineDir;
    private final long pubStepMs;
    private final boolean pubLoop;

    // Subscriber 用タイムライン設定
    private final File subTimelineDir;
    private final long subStepMs;
    private final boolean subLoop;

    public VehicleTimelineNode() {
        // AppConfig は既存の VehicleMain と同じ読み方に合わせる
        this.cfg = AppConfig.load();
        this.vehicleId = cfg.vehicleId;

        // REGION_TIMELINE_xxx
        String regionTimelineDirPath = System.getenv("REGION_TIMELINE_DIR");
        this.pubTimelineDir = (regionTimelineDirPath != null && !regionTimelineDirPath.isBlank())
                ? new File(regionTimelineDirPath)
                : null;
        long stepMsTmp;
        try {
            stepMsTmp = Long.parseLong(System.getenv().getOrDefault("REGION_TIMELINE_STEP_MS", "100"));
        } catch (NumberFormatException e) {
            stepMsTmp = 100L;
        }
        this.pubStepMs = stepMsTmp;
        this.pubLoop = "1".equals(System.getenv("REGION_TIMELINE_LOOP"));

        // SUB_TIMELINE_xxx
        String subTimelineDirPath = System.getenv("SUB_TIMELINE_DIR");
        this.subTimelineDir = (subTimelineDirPath != null && !subTimelineDirPath.isBlank())
                ? new File(subTimelineDirPath)
                : null;
        long subStepTmp;
        try {
            subStepTmp = Long.parseLong(System.getenv().getOrDefault("SUB_TIMELINE_STEP_MS", "100"));
        } catch (NumberFormatException e) {
            subStepTmp = 100L;
        }
        this.subStepMs = subStepTmp;
        this.subLoop = "1".equals(System.getenv("SUB_TIMELINE_LOOP"));
    }

    @Override
    public GraphName getDefaultNodeName() {
        // ノード名は vehicleId を含めておく
        String safeVehicleId = vehicleId.replaceAll("[^a-zA-Z0-9_]", "_");
        return GraphName.of("v2x_vehicle_timeline/" + safeVehicleId);
    }

    @Override
    public void onStart(final ConnectedNode connectedNode) {
        connectedNode.getLog().info(String.format(
                "[VehicleTimelineNode] start vehicleId=%s REGION_TIMELINE_DIR=%s SUB_TIMELINE_DIR=%s",
                vehicleId,
                (pubTimelineDir == null ? "null" : pubTimelineDir.getAbsolutePath()),
                (subTimelineDir == null ? "null" : subTimelineDir.getAbsolutePath())
        ));

        // ====== Publisher 側: JSON タイムライン + DatasetPointCloudSource ======
        if (pubTimelineDir != null && pubTimelineDir.isDirectory()) {
            final List<Path> pubFrames = listTimelineFiles(pubTimelineDir.toPath());
            if (pubFrames.isEmpty()) {
                connectedNode.getLog().error("[PUB-TL] no timeline json under " + pubTimelineDir.getAbsolutePath());
            } else {
                // RegionTimelineFeeder と同じように datasetRoot = timelineDir を使う
                final DatasetPointCloudSource datasetSource;
                try {
                    String datasetRoot = pubTimelineDir.getAbsolutePath();
                    datasetSource = new DatasetPointCloudSource(datasetRoot, cfg.datasetGlob, cfg.datasetLoop);
                } catch (Exception e) {
                    connectedNode.getLog().error("[PUB-TL] dataset not available: " + e.getMessage(), e);
                    // Publisher 側は諦める
                    return;
                }

                connectedNode.executeCancellableLoop(
                        new PublisherTimelineLoop(
                                connectedNode,
                                datasetSource,
                                pubFrames,
                                pubStepMs,
                                pubLoop,
                                vehicleId,
                                cfg.maxPointsPerChunk
                        )
                );
                connectedNode.getLog().info(String.format(
                        "[PUB-TL] started timeline dir=%s stepMs=%d loop=%s",
                        pubTimelineDir.getAbsolutePath(), pubStepMs, pubLoop
                ));
            }
        } else if (pubTimelineDir != null) {
            connectedNode.getLog().error("[PUB-TL] REGION_TIMELINE_DIR is not directory: " + pubTimelineDir.getAbsolutePath());
        } else {
            connectedNode.getLog().info("[PUB-TL] REGION_TIMELINE_DIR not set -> publisher timeline disabled.");
        }

        // ====== Subscriber 側: JSON タイムラインで欲しい領域集合を切り替え ======
        if (subTimelineDir != null && subTimelineDir.isDirectory()) {
            final List<Path> subFrames = listTimelineFiles(subTimelineDir.toPath());
            if (subFrames.isEmpty()) {
                connectedNode.getLog().error("[SUB-TL] no timeline json under " + subTimelineDir.getAbsolutePath());
            } else {
                connectedNode.executeCancellableLoop(
                        new SubscriberTimelineLoop(
                                connectedNode,
                                subFrames,
                                subStepMs,
                                subLoop
                        )
                );
                connectedNode.getLog().info(String.format(
                        "[SUB-TL] started timeline dir=%s stepMs=%d loop=%s",
                        subTimelineDir.getAbsolutePath(), subStepMs, subLoop
                ));
            }
        } else if (subTimelineDir != null) {
            connectedNode.getLog().error("[SUB-TL] SUB_TIMELINE_DIR is not directory: " + subTimelineDir.getAbsolutePath());
        } else {
            connectedNode.getLog().info("[SUB-TL] SUB_TIMELINE_DIR not set -> subscriber timeline disabled.");
        }
    }

    // ============================================================
    // Publisher 用 CancellableLoop
    // ===========================================================

    private static class PublisherTimelineLoop extends CancellableLoop {
        private final ConnectedNode node;
        private final DatasetPointCloudSource source;
        private final List<Path> frames;
        private final long stepMs;
        private final boolean loop;
        private final String vehicleId;
        private final int maxPointsPerChunk;

        private final Map<String, Publisher<ByteMultiArray>> active = new HashMap<>();
        private int frameIndex = 0;

        PublisherTimelineLoop(ConnectedNode node,
                              DatasetPointCloudSource source,
                              List<Path> frames,
                              long stepMs,
                              boolean loop,
                              String vehicleId,
                              int maxPointsPerChunk) {
            this.node = node;
            this.source = source;
            this.frames = frames;
            this.stepMs = stepMs;
            this.loop = loop;
            this.vehicleId = vehicleId;
            this.maxPointsPerChunk = maxPointsPerChunk;
        }

        @Override
        protected void loop() throws InterruptedException {
            if (frames.isEmpty()) {
                TimeUnit.SECONDS.sleep(1);
                return;
            }

            if (!loop && frameIndex >= frames.size()) {
                // 1 周したらそれ以上は何もしないで軽くスリープ
                TimeUnit.SECONDS.sleep(1);
                return;
            }

            int idx = frameIndex % frames.size();
            Path framePath = frames.get(idx);
            String frameName = framePath.getFileName().toString();
            node.getLog().info(String.format("[PUB-TL] frameIndex=%d file=%s", idx, frameName));

            List<String> regionList;
            try {
                regionList = readRegionsFromJson(framePath);
            } catch (IOException e) {
                node.getLog().error("[PUB-TL] failed to read timeline json: " + framePath, e);
                regionList = List.of();
            }

            LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);

            // ---- Publisher stop (active - current) ----
            Set<String> toStop = new HashSet<>(active.keySet());
            toStop.removeAll(currentRegions);
            if (!toStop.isEmpty()) {
                for (String regionId : toStop) {
                    Publisher<ByteMultiArray> pub = active.remove(regionId);
                    if (pub != null) {
                        node.getLog().info("[PUB-TL] stop publisher for region=" + regionId);
                        try {
                            pub.shutdown();
                        } catch (Exception ignore) {
                        }
                    }
                }
            }

            // ---- Publisher start (current - active) ----
            Set<String> toStart = new HashSet<>(currentRegions);
            toStart.removeAll(active.keySet());
            if (!toStart.isEmpty()) {
                for (String regionId : toStart) {
                    String safeRegionId = regionId.replaceAll("[^a-zA-Z0-9_]", "_");
                    String topic = "v2x/region/" + safeRegionId + "/data";
                    try {
                        Publisher<ByteMultiArray> pub =
                                node.newPublisher(topic, ByteMultiArray._TYPE);
                        active.put(regionId, pub);
                        node.getLog().info("[PUB-TL] started publisher for region=" + regionId + " topic=" + topic);
                    } catch (Exception e) {
                        node.getLog().error("[PUB-TL] failed to create publisher for region=" + regionId, e);
                    }
                }
            }

            // ---- 実際の送信 ----
            for (String regionId : currentRegions) {
                Publisher<ByteMultiArray> pub = active.get(regionId);
                if (pub == null) {
                    continue;
                }
                // Subscriber が 0 のときは無駄なので送らない (RosPublisher.hasSubscribers 相当)
                if (!pub.hasSubscribers()) {
                    // node.getLog().debug(...); // 必要なら詳細ログ
                    continue;
                }

                try {
                    PointCloudChunk chunk = source.nextChunkAtFrame(
                            vehicleId,
                            regionId,
                            idx,
                            maxPointsPerChunk
                    );
                    if (chunk == null) {
                        node.getLog().info("[DATASET] no more data for region=" + regionId);
                        continue;
                    }
                    int pointCount = (chunk.points() == null) ? 0 : chunk.points().size();
                    if (pointCount <= 10) {
                        // RegionTimelineFeeder と同じ閾値
                        continue;
                    }

                    byte[] payload = PointCloudSerializer.serialize(vehicleId, regionId, chunk);

                    ByteMultiArray msg = pub.newMessage();
                    // rosjava の ByteMultiArray は ChannelBuffer で data を扱う
                    ChannelBuffer buf = ChannelBuffers.copiedBuffer(ByteOrder.LITTLE_ENDIAN, payload);
                    msg.setData(buf);
                    pub.publish(msg);
                } catch (Exception e) {
                    node.getLog().error(
                            String.format("[PUB] error while sending frame=%d region=%s", idx, regionId),
                            e
                    );
                }
            }

            // フレーム間の待ち時間
            Thread.sleep(stepMs);
            frameIndex++;
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            // ノード終了時に Publisher を全部閉じる
            for (Publisher<ByteMultiArray> pub : active.values()) {
                try {
                    pub.shutdown();
                } catch (Exception ignore) {
                }
            }
        }
    }

    // ============================================================
    // Subscriber 用 CancellableLoop
    // ===========================================================

    private static class SubscriberTimelineLoop extends CancellableLoop {
        private final ConnectedNode node;
        private final List<Path> frames;
        private final long stepMs;
        private final boolean loop;

        private final Map<String, Subscriber<ByteMultiArray>> active = new HashMap<>();
        private int frameIndex = 0;

        SubscriberTimelineLoop(ConnectedNode node,
                               List<Path> frames,
                               long stepMs,
                               boolean loop) {
            this.node = node;
            this.frames = frames;
            this.stepMs = stepMs;
            this.loop = loop;
        }

        @Override
        protected void loop() throws InterruptedException {
            if (frames.isEmpty()) {
                TimeUnit.SECONDS.sleep(1);
                return;
            }

            if (!loop && frameIndex >= frames.size()) {
                TimeUnit.SECONDS.sleep(1);
                return;
            }

            int idx = frameIndex % frames.size();
            Path framePath = frames.get(idx);
            String frameName = framePath.getFileName().toString();
            node.getLog().info(String.format("[SUB-TL] frameIndex=%d file=%s", idx, frameName));

            List<String> regionList;
            try {
                regionList = readRegionsFromJson(framePath);
            } catch (IOException e) {
                node.getLog().error("[SUB-TL] failed to read timeline json: " + framePath, e);
                regionList = List.of();
            }

            LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);

            // ---- stop (active - current) ----
            Set<String> toStop = new HashSet<>(active.keySet());
            toStop.removeAll(currentRegions);
            for (String regionId : toStop) {
                Subscriber<ByteMultiArray> sub = active.remove(regionId);
                if (sub != null) {
                    node.getLog().info("[SUB-TL] stop subscriber for region=" + regionId + " at frameIndex=" + idx);
                    try {
                        sub.shutdown();
                    } catch (Exception ignore) {
                    }
                }
            }

            // ---- start (current - active) ----
            Set<String> toStart = new HashSet<>(currentRegions);
            toStart.removeAll(active.keySet());
            for (String regionId : toStart) {
                String topic = Topics.data(regionId);
                try {
                    Subscriber<ByteMultiArray> sub =
                            node.newSubscriber(topic, ByteMultiArray._TYPE);
                    sub.addMessageListener(new MessageListener<ByteMultiArray>() {
                        @Override
                        public void onNewMessage(ByteMultiArray message) {
                            try {
                                ChannelBuffer buf = message.getData();
                                byte[] payload = new byte[buf.readableBytes()];
                                buf.getBytes(buf.readerIndex(), payload);
                                PointCloudChunk chunk = PointCloudSerializer.deserialize(payload);

                                // 既存 SubscriberTask と同じログ + PCD 保存
                                System.out.println("[SUB] received chunk region=" + chunk.regionId()
                                        + " vehicleId=" + chunk.sourceVehicleId()
                                        + " ts=" + chunk.captureTsMillis()
                                        + " points=" + (chunk.points() == null ? 0 : chunk.points().size()));
                                saveChunkAsPcd(chunk);
                            } catch (Exception e) {
                                node.getLog().error("[SUB] failed to handle incoming message", e);
                            }
                        }
                    });
                    active.put(regionId, sub);
                    node.getLog().info("[SUB-TL] started subscriber for region=" + regionId
                            + " topic=" + topic + " at frameIndex=" + idx);
                } catch (Exception e) {
                    node.getLog().error("[SUB-TL] failed to create subscriber for region=" + regionId, e);
                }
            }

            Thread.sleep(stepMs);
            frameIndex++;
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            for (Subscriber<ByteMultiArray> sub : active.values()) {
                try {
                    sub.shutdown();
                } catch (Exception ignore) {
                }
            }
        }
    }

    // ============================================================
    // 共通ユーティリティ (RegionTimelineFeeder / SubscriberTask から移植)
    // ===========================================================

    private static List<Path> listTimelineFiles(Path dir) {
        File[] files = dir.toFile().listFiles((d, name) -> name.endsWith(".json"));
        if (files == null) {
            return List.of();
        }
        return Arrays.stream(files)
                .map(File::toPath)
                .sorted(Comparator.comparing(Path::getFileName))
                .collect(Collectors.toList());
    }

    /**
     * JSON から領域一覧を読む。
     * - ["cell-0a3c", "cell-0a3d"]
     * - {"regions":["cell-0a3c","cell-0a3d"]}
     * の両方を許容。
     */
    private static List<String> readRegionsFromJson(Path jsonPath) throws IOException {
        String text = Files.readString(jsonPath, StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) {
            return List.of();
        }
        // 配列だけの形式 ["cell-0a3c", ...]
        if (text.startsWith("[")) {
            String[] arr = Jsons.GSON.fromJson(text, String[].class);
            return Arrays.asList(arr);
        }
        // オブジェクト形式 {"regions":[...]} を想定
        JsonObject obj = Jsons.GSON.fromJson(text, JsonObject.class);
        JsonElement regionsElem = obj.get("regions");
        if (regionsElem == null || !regionsElem.isJsonArray()) {
            return List.of();
        }
        JsonArray arr = regionsElem.getAsJsonArray();
        List<String> result = new ArrayList<>(arr.size());
        for (JsonElement e : arr) {
            if (e.isJsonPrimitive()) {
                result.add(e.getAsString());
            }
        }
        return result;
    }

    /**
     * 既存の SubscriberTask と同じ処理:
     * 受信したチャンクを /home/tavivid/v2x-pcd/received/{regionId}/{fileName}.pcd に保存。
     */
    private static void saveChunkAsPcd(PointCloudChunk chunk) {
        List<?> pts = chunk.points();
        if (pts == null || pts.isEmpty()) {
            System.out.println("[SUB] skip saving empty chunk region=" + chunk.regionId());
            return;
        }

        File baseDir = new File("/home/tavivid/v2x-pcd/received");
        File regionDir = new File(baseDir, chunk.regionId());
        if (!regionDir.exists() && !regionDir.mkdirs()) {
            System.err.println("[SUB] failed to create dir: " + regionDir.getAbsolutePath());
            return;
        }

        String fileName;
        if (chunk.sourceFileName() != null && !chunk.sourceFileName().isBlank()) {
            fileName = chunk.sourceFileName();
        } else {
            fileName = chunk.captureTsMillis() + ".pcd";
        }
        File out = new File(regionDir, fileName);

        int numPoints = pts.size();
        try (BufferedWriter w = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.US_ASCII))) {

            w.write("# .PCD v0.7\n");
            w.write("VERSION 0.7\n");
            w.write("FIELDS x y z\n");
            w.write("SIZE 4 4 4\n");
            w.write("TYPE F F F\n");
            w.write("COUNT 1 1 1\n");
            w.write("WIDTH " + numPoints + "\n");
            w.write("HEIGHT 1\n");
            w.write("VIEWPOINT 0 0 0 1 0 0 0\n");
            w.write("POINTS " + numPoints + "\n");
            w.write("DATA ascii\n");

            for (Object o : pts) {
                if (!(o instanceof Point3D)) {
                    continue;
                }
                Point3D p = (Point3D) o;
                w.write(p.x() + " " + p.y() + " " + p.z());
                w.newLine();
            }
        } catch (IOException e) {
            System.err.println("[SUB] failed to save PCD file: " + out.getAbsolutePath());
            e.printStackTrace();
            return;
        }
        System.out.println("[SUB] saved PCD file: " + out.getAbsolutePath());
    }
}
