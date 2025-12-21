package v2x.vehicle.ros.timeline;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.ros.concurrent.CancellableLoop;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Publisher;
import std_msgs.ByteMultiArray;
import v2x.vehicle.datasource.DatasetPointCloudSource;

import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

public final class PublisherTimelineLoop extends CancellableLoop {

    private final ConnectedNode node;
    private final DatasetPointCloudSource source;
    private final List<Path> frames;
    private final long stepMs;
    private final boolean loop;
    private final String vehicleId; // 現状未使用だが互換のため保持
    private final int maxPointsPerChunk; // 現状未使用だが互換のため保持
    private final Map<String, Publisher<ByteMultiArray>> publisherPool;

    private final Map<String, Publisher<ByteMultiArray>> active = new HashMap<>();
    private int frameIndex = 0;

    private final long syncUnixSec;

    // sendNano を共有メモリ(mmap)で渡す（hashキー）
    private static final SharedLatencyStore LAT_STORE = new SharedLatencyStore();

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
            return node.newPublisher(topic, ByteMultiArray._TYPE);
        });
    }

    @Override
    protected void loop() throws InterruptedException {
        // 同期開始時刻まで待つ（任意）
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
            TimelineLog.logf("PUB-TL", "timeline finished.\nstopping publisher loop.\nframes=%d", frames.size());
            this.cancel();
            return;
        }

        int idx = frameIndex % frames.size();
        Path framePath = frames.get(idx);
        String frameName = framePath.getFileName().toString();
        TimelineLog.logf("PUB-TL", "reading timeline frame=%d file=%s", idx, frameName);

        List<String> regionList;
        try {
            regionList = TimelineFiles.readRegionsFromJson(framePath);
        } catch (Exception e) {
            TimelineLog.error("PUB-TL", "failed to read timeline json: " + framePath, e);
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
                // RAW_ONLY 専用: PCD本体だけ送る
                DatasetPointCloudSource.RawPcd rawPcd = source.readRawPcdWithName(regionId, idx);
                if (rawPcd == null || rawPcd.data == null) {
                    TimelineLog.logf("PUB", "skip RAW frame=%d region=%s", idx, regionId);
                    continue;
                }

                long sendNano = System.nanoTime();

                ChannelBuffer buf = ChannelBuffers.buffer(ByteOrder.LITTLE_ENDIAN, rawPcd.data.length);
                buf.writeBytes(rawPcd.data);

                ByteMultiArray msg = pub.newMessage();
                msg.setData(buf);
                pub.publish(msg);

                // hashでsendNanoを共有メモリに記録
                int hash = crc32c(rawPcd.data);
                LAT_STORE.putSendNanoByHash(regionId, hash, sendNano);

                String topic = TimelineTopics.topicForRegion(regionId);
                TimelineLog.logf(
                        "PUB",
                        "sent PCD frame=%d region=%s file=%s bytes=%d topic=%s sendNano=%d",
                        idx, regionId, rawPcd.fileName, rawPcd.data.length, topic, sendNano
                );

            } catch (Exception e) {
                TimelineLog.error("PUB", "error while sending frame=" + idx + " region=" + regionId, e);
            }
        }

        Thread.sleep(stepMs);
        frameIndex++;
    }

    private static int crc32c(byte[] data) {
        CRC32C c = new CRC32C();
        c.update(data, 0, data.length);
        return (int) c.getValue();
    }
}
