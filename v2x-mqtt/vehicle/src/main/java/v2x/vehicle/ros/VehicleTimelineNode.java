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
import v2x.vehicle.util.Jsons;
import v2x.vehicle.util.PointCloudSerializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class VehicleTimelineNode extends AbstractNodeMain {

    private final AppConfig cfg;
    private final String vehicleId;

    // Publisher timeline config
    private final File pubTimelineDir;
    private final long pubStepMs;
    private final boolean pubLoop;

    // Subscriber timeline config
    private final File subTimelineDir;
    private final long subStepMs;
    private final boolean subLoop;
    private final long timelineSyncUnixSec;

    // ★ publisher プール（region → Publisher）
    private final Map<String, Publisher<ByteMultiArray>> publisherPool = new HashMap<>();

    public VehicleTimelineNode() {
        this.cfg = AppConfig.load();
        this.vehicleId = cfg.vehicleId;

        // PUB timeline config
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
        String pubLoopEnv = System.getenv().getOrDefault("REGION_TIMELINE_LOOP", "0");
        this.pubLoop = "1".equals(pubLoopEnv);

        // SUB timeline config
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
        String subLoopEnv = System.getenv().getOrDefault("SUB_TIMELINE_LOOP", "0");
        this.subLoop = "1".equals(subLoopEnv);

        long syncTmp;
        try {
            syncTmp = Long.parseLong(System.getenv().getOrDefault("TIMELINE_SYNC_UNIX", "0"));
        } catch (NumberFormatException e) {
            syncTmp = 0L;
        }
        this.timelineSyncUnixSec = syncTmp;
    }

    @Override
    public GraphName getDefaultNodeName() {
        String safeVehicleId = vehicleId.replaceAll("[^a-zA-Z0-9_]", "_");
        return GraphName.of("v2x_vehicle_timeline/" + safeVehicleId);
    }

    @Override
    public void onStart(final ConnectedNode connectedNode) {

        // =====================================================================
        // Publisher timeline
        // =====================================================================
        if (pubTimelineDir != null && pubTimelineDir.isDirectory()) {
            final List<Path> pubFrames = listTimelineFiles(pubTimelineDir.toPath());
            if (pubFrames.isEmpty()) {
                connectedNode.getLog().error("[PUB-TL] no timeline json under " + pubTimelineDir.getAbsolutePath());
            } else {
                final DatasetPointCloudSource datasetSource;
                try {
                    String datasetRoot = pubTimelineDir.getAbsolutePath();
                    datasetSource = new DatasetPointCloudSource(datasetRoot, cfg.datasetGlob, cfg.datasetLoop);
                } catch (Exception e) {
                    connectedNode.getLog().error("[PUB-TL] dataset not available: " + e.getMessage(), e);
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
                                cfg.maxPointsPerChunk,
                                publisherPool,
                                timelineSyncUnixSec

                        )
                );
                VehicleTimelineNode.logf("PUB-TL",
                        "started timeline dir=%s stepMs=%d loop=%s syncUnix=%d",
                        pubTimelineDir.getAbsolutePath(), pubStepMs, pubLoop, timelineSyncUnixSec);
            }
        }

        // =====================================================================
        // Subscriber timeline
        // =====================================================================
        if (subTimelineDir != null && subTimelineDir.isDirectory()) {
            final List<Path> subFrames = listTimelineFiles(subTimelineDir.toPath());
            if (!subFrames.isEmpty()) {
                connectedNode.executeCancellableLoop(
                        new SubscriberTimelineLoop(
                                connectedNode,
                                subFrames,
                                subStepMs,
                                subLoop,
                                timelineSyncUnixSec
                        )
                );
                VehicleTimelineNode.logf("SUB-TL",
                        "started timeline dir=%s stepMs=%d loop=%s syncUnix=%d",
                        subTimelineDir.getAbsolutePath(), subStepMs, subLoop, timelineSyncUnixSec);
            }
        }
    }

    // =========================================================================
    // PublisherTimelineLoop
    // =========================================================================
    private static class PublisherTimelineLoop extends CancellableLoop {

        private final ConnectedNode node;
        private final DatasetPointCloudSource source;
        private final List<Path> frames;
        private final long stepMs;
        private final boolean loop;
        private final String vehicleId;
        private final int maxPointsPerChunk;

        // ★プール（VehicleTimelineNode から渡される）
        private final Map<String, Publisher<ByteMultiArray>> publisherPool;

        private final Map<String, Publisher<ByteMultiArray>> active = new HashMap<>();
        private int frameIndex = 0;
        private final long syncUnixSec;

        PublisherTimelineLoop(
                ConnectedNode node,
                DatasetPointCloudSource source,
                List<Path> frames,
                long stepMs,
                boolean loop,
                String vehicleId,
                int maxPointsPerChunk,
                Map<String, Publisher<ByteMultiArray>> publisherPool,
                long syncUnixSec
        ) {
            this.node = node;
            this.source = source;
            this.frames = frames;
            this.stepMs = stepMs;
            this.loop = loop;
            this.vehicleId = vehicleId;
            this.maxPointsPerChunk = maxPointsPerChunk;
            this.publisherPool = publisherPool;
            this.syncUnixSec = syncUnixSec;
        }

        // ★ Publisher を取得（初回のみ newPublisher）
        private Publisher<ByteMultiArray> getOrCreatePublisher(String regionId) {
            return publisherPool.computeIfAbsent(regionId, r -> {
                String topic = VehicleTimelineNode.topicForRegion(r);
                VehicleTimelineNode.logf("PUB-TL",
                        "create publisher for region=%s topic=%s", r, topic);
                node.getLog().info("[PUB-TL] create publisher for region=" + r + " topic=" + topic);

                Publisher<ByteMultiArray> pub = node.newPublisher(topic, ByteMultiArray._TYPE);
                return pub;
            });
        }

        @Override
        protected void loop() throws InterruptedException {

            if (syncUnixSec > 0L) {
                long nowSec = System.currentTimeMillis() / 1000L;
                if (nowSec < syncUnixSec) {
                    long remainMs = (syncUnixSec - nowSec) * 1000L;
                    // 細かく起こしすぎないように stepMs か残り時間分だけスリープ
                    TimeUnit.MILLISECONDS.sleep(Math.min(stepMs, remainMs));
                    return;
                }
            }

            if (frames.isEmpty()) {
                TimeUnit.SECONDS.sleep(1);
                return;
            }
            if (!loop && frameIndex >= frames.size()) {
                VehicleTimelineNode.logf("PUB-TL",
                        "timeline finished. stopping publisher loop. frames=%d", frames.size());
                this.cancel();
                return;
            }

            if (frameIndex == 0) {
                VehicleTimelineNode.resetElapsedBase();
            }

            int idx = frameIndex % frames.size();
            Path framePath = frames.get(idx);

            String frameName = framePath.getFileName().toString();
            VehicleTimelineNode.logf("PUB-TL",
                    "reading timeline frame=%d file=%s", idx, frameName);

            List<String> regionList;
            try {
                regionList = readRegionsFromJson(framePath);
            } catch (IOException e) {
                node.getLog().error("[PUB-TL] failed to read timeline json: " + framePath, e);
                regionList = List.of();
            }

            LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);

            // ----------------------------
            // active set 更新（stop はしない）
            // ----------------------------
            Set<String> before = new HashSet<>(active.keySet());

            active.keySet().retainAll(currentRegions);
            Set<String> removed = new HashSet<>(before);
            removed.removeAll(active.keySet());
            for (String r : removed) {
                VehicleTimelineNode.logf("PUB-TL", "region inactive: %s", r);
            }

            for (String r : currentRegions) {
                if (!active.containsKey(r)) {
                    active.put(r, getOrCreatePublisher(r));
                    VehicleTimelineNode.logf("PUB-TL", "region active: %s", r);
                }
            }

            // ----------------------------
            // publish
            // ----------------------------
            for (String regionId : currentRegions) {
                Publisher<ByteMultiArray> pub = active.get(regionId);
                if (pub == null) continue;

                if (!pub.hasSubscribers()) {
                    continue;
                }

                try {
                    // ★ファイル名＋中身を取得
                    DatasetPointCloudSource.RawPcd rawPcd =
                            source.readRawPcdWithName(regionId, idx);
                    if (rawPcd == null || rawPcd.data == null) {
                        VehicleTimelineNode.logf("PUB",
                                "skip RAW frame=%d region=%s (no data)", idx, regionId);
                        continue;
                    }

                    byte[] nameBytes = rawPcd.fileName.getBytes(StandardCharsets.UTF_8);

                    // [4byte: ファイル名長(int, LE)] [ファイル名UTF-8] [PCD本体]
                    int totalLen = 4 + nameBytes.length + rawPcd.data.length;
                    ChannelBuffer buf = ChannelBuffers.buffer(ByteOrder.LITTLE_ENDIAN, totalLen);
                    buf.writeInt(nameBytes.length);
                    buf.writeBytes(nameBytes);
                    buf.writeBytes(rawPcd.data);

                    ByteMultiArray msg = pub.newMessage();
                    msg.setData(buf);
                    pub.publish(msg);

                    String topic = VehicleTimelineNode.topicForRegion(regionId);
                    VehicleTimelineNode.logf("PUB",
                            "sent RAW frame=%d region=%s file=%s bytes=%d topic=%s",
                            idx, regionId, rawPcd.fileName, totalLen, topic);

                } catch (Exception e) {
                    node.getLog().error(
                            "[PUB] error while sending frame=" + idx + " region=" + regionId,
                            e
                    );
                }
            }

            Thread.sleep(stepMs);
            frameIndex++;
        }
    }

    // =========================================================================
    // SubscriberTimelineLoop（これは現状維持）
    // =========================================================================
    private static class SubscriberTimelineLoop extends CancellableLoop {

        private final ConnectedNode node;
        private final List<Path> frames;
        private final long stepMs;
        private final boolean loop;

        private final Map<String, Subscriber<ByteMultiArray>> active = new HashMap<>();
        private int frameIndex = 0;
        private final long syncUnixSec;

        SubscriberTimelineLoop(ConnectedNode node,
                               List<Path> frames,
                               long stepMs,
                               boolean loop,
                               long syncUnixSec) {
            this.node = node;
            this.frames = frames;
            this.stepMs = stepMs;
            this.loop = loop;
            this.syncUnixSec = syncUnixSec;
        }

        @Override
        protected void loop() throws InterruptedException {

            if (syncUnixSec > 0L) {
                long nowSec = System.currentTimeMillis() / 1000L;
                if (nowSec < syncUnixSec) {
                    long remainMs = (syncUnixSec - nowSec) * 1000L;
                    TimeUnit.MILLISECONDS.sleep(Math.min(stepMs, remainMs));
                    return;
                }
            }

            if (frames.isEmpty()) {
                TimeUnit.SECONDS.sleep(1);
                return;
            }
            if (!loop && frameIndex >= frames.size()) {
                VehicleTimelineNode.logf("SUB-TL",
                        "timeline finished. stopping subscriber loop. frames=%d", frames.size());
                this.cancel();
                return;
            }

            if (frameIndex == 0) {
                VehicleTimelineNode.resetElapsedBase();
            }
            int idx = frameIndex % frames.size();
            Path framePath = frames.get(idx);
            String frameName = framePath.getFileName().toString();

            VehicleTimelineNode.logf("SUB-TL",
                    "reading timeline frame=%d file=%s", idx, frameName);

            List<String> regionList;
            try {
                regionList = readRegionsFromJson(framePath);
            } catch (IOException e) {
                node.getLog().error("[SUB-TL] failed to read timeline json: " + framePath, e);
                regionList = List.of();
            }

            LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);
            VehicleTimelineNode.logf("SUB-TL",
                    "currentRegions=%s", currentRegions.toString());

            // stop
            Set<String> toStop = new HashSet<>(active.keySet());
            toStop.removeAll(currentRegions);
            for (String r : toStop) {
                Subscriber<ByteMultiArray> sub = active.remove(r);
                if (sub != null) {
                    VehicleTimelineNode.logf("SUB-TL",
                            "stop subscriber for region=%s", r);
                    try {
                        sub.shutdown();
                    } catch (Exception ignore) {
                    }
                }
            }

            // start
            Set<String> toStart = new HashSet<>(currentRegions);
            toStart.removeAll(active.keySet());
            VehicleTimelineNode.logf("SUB-TL",
                    "toStart=%s", toStart.toString());

            for (String r : toStart) {
                String topic = VehicleTimelineNode.topicForRegion(r);
                VehicleTimelineNode.logf("SUB-TL",
                        "start subscriber for region=%s topic=%s", r, topic);
                try {
                    Subscriber<ByteMultiArray> sub =
                            node.newSubscriber(topic, ByteMultiArray._TYPE);
                    sub.addMessageListener(message -> {
                        try {
                            ChannelBuffer buf = message.getData();
                            int len = (buf != null) ? buf.readableBytes() : -1;
                            VehicleTimelineNode.logf("SUB", "raw message arrived for region=%s len=%d", r, len);
                            if (buf == null) {
                                throw new IllegalStateException("ByteMultiArray.getData() returned null");
                            }

                            // ChannelBuffer から直接ヘッダを読む（readerIndex はいじらない）
                            if (len < 4) {
                                throw new IllegalStateException("payload too short for header");
                            }

                            ChannelBuffer dup = buf.duplicate();
                            int headerIndex = dup.readerIndex();
                            int nameLen = dup.getInt(headerIndex);
                            if (nameLen < 0 || 4 + nameLen > len) {
                                throw new IllegalStateException(
                                        "invalid nameLen=" + nameLen + " payloadLen=" + len);
                            }

                            int nameOffset = headerIndex + 4;
                            byte[] nameBytes = new byte[nameLen];
                            dup.getBytes(nameOffset, nameBytes);
                            String fileName = new String(nameBytes, StandardCharsets.UTF_8);

                            int pcdOffset = nameOffset + nameLen;
                            int pcdLen = len - (4 + nameLen);
                            if (pcdLen <= 0) {
                                throw new IllegalStateException("no PCD body in payload");
                            }
                            byte[] rawPcd = new byte[pcdLen];
                            dup.getBytes(pcdOffset, rawPcd);

                            String region = r;
                            File base = new File("/home/Tavivid/v2x-pcd/received_raw");
                            File outDir = new File(base, region);
                            outDir.mkdirs();

                            // ★送信元と同じファイル名で保存
                            File out = new File(outDir, fileName);
                            Files.write(out.toPath(), rawPcd);

                            System.out.println("[SUB] saved RAW PCD: " + out.getAbsolutePath());

                        } catch (Exception e) {
                            node.getLog().error("[SUB] failed to handle incoming message", e);
                        }
                    });
                    active.put(r, sub);
                } catch (Exception e) {
                    System.err.println("[SUB] exception while handling incoming message for region=" + r);
                    e.printStackTrace();
                    node.getLog().error("[SUB] failed to handle incoming message", e);
                }
            }

            Thread.sleep(stepMs);
            frameIndex++;
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================
    private static List<Path> listTimelineFiles(Path dir) {
        File[] files = dir.toFile().listFiles((d, name) -> name.endsWith(".json"));
        if (files == null) return List.of();
        return Arrays.stream(files)
                .map(File::toPath)
                .sorted(Comparator.comparing(Path::getFileName))
                .collect(Collectors.toList());
    }

    private static List<String> readRegionsFromJson(Path jsonPath) throws IOException {
        String text = Files.readString(jsonPath, StandardCharsets.UTF_8).trim();
        if (text.isEmpty()) return List.of();

        if (text.startsWith("[")) {
            String[] arr = Jsons.GSON.fromJson(text, String[].class);
            return Arrays.asList(arr);
        }

        JsonObject obj = Jsons.GSON.fromJson(text, JsonObject.class);
        JsonElement e = obj.get("regions");
        if (e == null || !e.isJsonArray()) return List.of();

        JsonArray arr = e.getAsJsonArray();
        List<String> result = new ArrayList<>(arr.size());
        for (JsonElement je : arr) {
            if (je.isJsonPrimitive()) result.add(je.getAsString());
        }
        return result;
    }

    private static void saveChunkAsPcd(PointCloudChunk chunk) {

        List<?> pts = chunk.points();
        if (pts == null || pts.isEmpty()) {
            System.out.println("[SUB] skip saving empty chunk region=" + chunk.regionId());
            return;
        }

        File baseDir = new File("/home/Tavivid/v2x-pcd/received");
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            System.err.println("[SUB] failed to create base dir: " + baseDir.getAbsolutePath());
            return;
        }

        File regionDir = new File(baseDir, chunk.regionId());
        if (!regionDir.exists() && !regionDir.mkdirs()) {
            System.err.println("[SUB] failed to create region dir: " + regionDir.getAbsolutePath());
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
                if (o instanceof Point3D p) {
                    w.write(p.x() + " " + p.y() + " " + p.z());
                    w.newLine();
                }
                Point3D p = (Point3D) o;
                w.write(p.x() + " " + p.y() + " " + p.z());
                w.newLine();
            }

        } catch (IOException e) {
            System.err.println("[SUB] failed to save PCD file: " + out.getAbsolutePath());
            e.printStackTrace();
        }
        System.out.println("[SUB] saved PCD file: " + out.getAbsolutePath());
    }

    private static volatile long baseStartTimeMs = -1L;

    private static void resetElapsedBase() {
        baseStartTimeMs = System.currentTimeMillis();
    }

    private static void initBaseStartTimeIfNeeded() {
        if (baseStartTimeMs < 0) {
            baseStartTimeMs = System.currentTimeMillis();
        }
    }

    private static String elapsed() {
        initBaseStartTimeIfNeeded();
        long ms = System.currentTimeMillis() - baseStartTimeMs;
        long sec = ms / 1000;
        long ms2 = ms % 1000;
        return String.format("[%02d:%02d.%03d]", sec / 60, sec % 60, ms2);
    }

    private static void log(String tag, String msg) {
        System.out.printf("%s [%s] %s%n", elapsed(), tag, msg);
    }

    private static void logf(String tag, String fmt, Object... args) {
        String body = String.format(fmt, args);
        log(tag, body);
    }

    private static String topicForRegion(String regionId) {
        String safeRegionId = regionId.replaceAll("[^a-zA-Z0-9_]", "_");
        return "/v2x/region/" + safeRegionId + "/data";
    }

}
