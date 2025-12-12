package v2x.vehicle.ros.timeline;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.ros.concurrent.CancellableLoop;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Publisher;
import std_msgs.ByteMultiArray;
import v2x.vehicle.datasource.DatasetPointCloudSource;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class PublisherTimelineLoop extends CancellableLoop {

    private final ConnectedNode node;
    private final DatasetPointCloudSource source;
    private final List<Path> frames;
    private final long stepMs;
    private final boolean loop;
    private final String vehicleId;
    private final int maxPointsPerChunk;

    private final Map<String, Publisher<ByteMultiArray>> publisherPool;
    private final Map<String, Publisher<ByteMultiArray>> active = new HashMap<>();

    private int frameIndex = 0;
    private final long syncUnixSec;

    public PublisherTimelineLoop(
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

    private Publisher<ByteMultiArray> getOrCreatePublisher(String regionId) {
        return publisherPool.computeIfAbsent(regionId, r -> {
            String topic = TimelineTopics.topicForRegion(r);
            TimelineLog.logf("PUB-TL", "create publisher for region=%s topic=%s", r, topic);
            node.getLog().info("[PUB-TL] create publisher for region=" + r + " topic=" + topic);
            return node.newPublisher(topic, ByteMultiArray._TYPE);
        });
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
            TimelineLog.logf("PUB-TL", "timeline finished. stopping publisher loop. frames=%d", frames.size());
            this.cancel();
            return;
        }

        if (frameIndex == 0) {
            TimelineLog.resetElapsedBase();
        }

        int idx = frameIndex % frames.size();
        Path framePath = frames.get(idx);
        String frameName = framePath.getFileName().toString();
        TimelineLog.logf("PUB-TL", "reading timeline frame=%d file=%s", idx, frameName);

        List<String> regionList;
        try {
            regionList = TimelineFiles.readRegionsFromJson(framePath);
        } catch (Exception e) {
            node.getLog().error("[PUB-TL] failed to read timeline json: " + framePath, e);
            regionList = List.of();
        }

        LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);

        // active set 更新（stop はしない）
        Set<String> before = new HashSet<>(active.keySet());
        active.keySet().retainAll(currentRegions);

        Set<String> removed = new HashSet<>(before);
        removed.removeAll(active.keySet());
        for (String r : removed) {
            TimelineLog.logf("PUB-TL", "region inactive: %s", r);
        }

        for (String r : currentRegions) {
            if (!active.containsKey(r)) {
                active.put(r, getOrCreatePublisher(r));
                TimelineLog.logf("PUB-TL", "region active: %s", r);
            }
        }

        // publish
        for (String regionId : currentRegions) {
            Publisher<ByteMultiArray> pub = active.get(regionId);
            if (pub == null) continue;
            if (!pub.hasSubscribers()) continue;

            try {
                // ★ファイル名＋中身を取得（あなたの現行仕様）
                DatasetPointCloudSource.RawPcd rawPcd = source.readRawPcdWithName(regionId, idx);
                if (rawPcd == null || rawPcd.data == null) {
                    TimelineLog.logf("PUB", "skip RAW frame=%d region=%s (no data)", idx, regionId);
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

                String topic = TimelineTopics.topicForRegion(regionId);
                TimelineLog.logf("PUB",
                        "sent RAW frame=%d region=%s file=%s bytes=%d topic=%s",
                        idx, regionId, rawPcd.fileName, totalLen, topic);

            } catch (Exception e) {
                node.getLog().error("[PUB] error while sending frame=" + idx + " region=" + regionId, e);
            }
        }

        Thread.sleep(stepMs);
        frameIndex++;
    }
}
