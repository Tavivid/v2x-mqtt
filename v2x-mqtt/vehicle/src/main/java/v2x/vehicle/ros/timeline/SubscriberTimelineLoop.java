package v2x.vehicle.ros.timeline;

import org.jboss.netty.buffer.ChannelBuffer;
import org.ros.concurrent.CancellableLoop;
import org.ros.node.ConnectedNode;
import org.ros.node.topic.Subscriber;
import std_msgs.ByteMultiArray;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class SubscriberTimelineLoop extends CancellableLoop {

    private final ConnectedNode node;
    private final List<Path> frames;
    private final long stepMs;
    private final boolean loop;
    private final Map<String, Subscriber<ByteMultiArray>> active = new HashMap<>();
    private int frameIndex = 0;
    private final long syncUnixSec;

    // ★共有メモリで sendNano を受け取る
    private static final SharedLatencyStore LAT_STORE = new SharedLatencyStore();

    // ★受信payloadのモード（RAW_ONLYならPCD本体だけ）
    private static final String PCD_PAYLOAD_MODE
            = System.getenv().getOrDefault("PCD_PAYLOAD_MODE", "NAMED_WITH_SENDNANO");
    private final boolean rawOnly = "RAW_ONLY".equalsIgnoreCase(PCD_PAYLOAD_MODE);

    // ★RAW_ONLY時の保存ファイル名連番（regionごと）
    private final Map<String, Integer> rawOnlySeqByRegion = new HashMap<>();

    public SubscriberTimelineLoop(
            ConnectedNode node,
            List<Path> frames,
            long stepMs,
            boolean loop,
            long syncUnixSec
    ) {
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
            TimelineLog.logf("SUB-TL", "timeline finished.\nstopping subscriber loop.\nframes=%d", frames.size());
            this.cancel();
            return;
        }

        int idx = frameIndex % frames.size();
        Path framePath = frames.get(idx);
        String frameName = framePath.getFileName().toString();
        TimelineLog.logf("SUB-TL", "reading timeline frame=%d file=%s", idx, frameName);

        List<String> regionList;
        try {
            regionList = TimelineFiles.readRegionsFromJson(framePath);
        } catch (Exception e) {
            TimelineLog.error("SUB-TL", "failed to read timeline json: " + framePath, e);
            regionList = List.of();
        }

        LinkedHashSet<String> currentRegions = new LinkedHashSet<>(regionList);
        TimelineLog.logf("SUB-TL", "currentRegions=%s", currentRegions);

        // stop
        Set<String> toStop = new HashSet<>(active.keySet());
        toStop.removeAll(currentRegions);
        for (String r : toStop) {
            Subscriber<ByteMultiArray> sub = active.remove(r);
            if (sub != null) {
                TimelineLog.logf("SUB-TL", "stop subscriber for region=%s", r);
                try {
                    sub.shutdown();
                } catch (Exception ignore) {
                }
            }
        }

        // start
        Set<String> toStart = new HashSet<>(currentRegions);
        toStart.removeAll(active.keySet());
        TimelineLog.logf("SUB-TL", "toStart=%s", toStart);

        for (String r : toStart) {
            String topic = TimelineTopics.topicForRegion(r);
            TimelineLog.logf("SUB-TL", "start subscriber for region=%s topic=%s", r, topic);

            try {
                Subscriber<ByteMultiArray> sub = node.newSubscriber(topic, ByteMultiArray._TYPE);
                sub.addMessageListener(message -> {
                    // 従来経路の recvNano は従来どおり（受信コールバックに入った時刻）
                    long recvNano = System.nanoTime();

                    try {
                        ChannelBuffer buf = message.getData();
                        int len = (buf != null) ? buf.readableBytes() : -1;
                        if (buf == null) {
                            throw new IllegalStateException("ByteMultiArray.getData() returned null");
                        }
                        if (len <= 0) {
                            throw new IllegalStateException("empty payload");
                        }

                        ChannelBuffer dup = buf.duplicate();
                        int headerIndex = dup.readerIndex();

                        final String fileName;
                        final byte[] rawPcd;

                        // ★RAW_ONLYのときだけ seq を保持して、あとで共有メモリから sendNano を引く
                        Integer rawOnlySeq = null;

                        if (rawOnly) {
                            // ★RAW_ONLY: payload全体をPCD本体として扱う
                            int seq;
                            synchronized (rawOnlySeqByRegion) {
                                seq = rawOnlySeqByRegion.getOrDefault(r, 0);
                                rawOnlySeqByRegion.put(r, seq + 1);
                            }
                            rawOnlySeq = seq;

                            fileName = String.format("%06d.pcd", seq);

                            rawPcd = new byte[len];
                            dup.getBytes(headerIndex, rawPcd);

                        } else {
                            // ★従来: [8:sendNano][4:nameLen][name][pcd]
                            if (len < 12) {
                                throw new IllegalStateException("payload too short for header: len=" + len);
                            }

                            long sendNano = dup.getLong(headerIndex);
                            int nameLen = dup.getInt(headerIndex + 8);

                            if (nameLen < 0 || 12 + nameLen > len) {
                                throw new IllegalStateException("invalid nameLen=" + nameLen + " payloadLen=" + len);
                            }

                            int nameOffset = headerIndex + 12;
                            byte[] nameBytes = new byte[nameLen];
                            dup.getBytes(nameOffset, nameBytes);
                            fileName = new String(nameBytes, StandardCharsets.UTF_8);

                            int pcdOffset = nameOffset + nameLen;
                            int pcdLen = len - (12 + nameLen);
                            if (pcdLen <= 0) {
                                throw new IllegalStateException("no PCD body in payload");
                            }

                            rawPcd = new byte[pcdLen];
                            dup.getBytes(pcdOffset, rawPcd);

                            long latencyNs = recvNano - sendNano;
                            double latencyMs = latencyNs / 1_000_000.0;

                            // ★指定フォーマット維持（RAW_ONLY以外）
                            File base = new File("/home/tavivid/v2x-pcd/received_raw");
                            File outDir = new File(base, r);
                            outDir.mkdirs();
                            File out = new File(outDir, fileName);
                            Files.write(out.toPath(), rawPcd);

                            TimelineLog.logf(
                                    "SUB",
                                    "saved PCD: %s bytes=%d latency_ns=%d latency_ms=%.3f sendNano=%d recvNano=%d",
                                    out.getAbsolutePath(), len, latencyNs, latencyMs, sendNano, recvNano
                            );
                            return; // ★従来経路はここで終了
                        }

                        // ===== RAW_ONLY 経路 =====
                        // 保存
                        File base = new File("/home/tavivid/v2x-pcd/received_raw");
                        File outDir = new File(base, r);
                        outDir.mkdirs();
                        File out = new File(outDir, fileName);
                        Files.write(out.toPath(), rawPcd);

                        // 「保存完了」の時刻（要件）
                        long saveNano = System.nanoTime();

                        // 共有メモリから sendNano を引く（publisher側が同じ region/seq で put している前提）
                        Long sendNano = (rawOnlySeq == null) ? null : LAT_STORE.getSendNano(r, rawOnlySeq);

                        if (sendNano != null) {
                            long latencyNs = saveNano - sendNano;
                            double latencyMs = latencyNs / 1_000_000.0;

                            // できるだけ既存フォーマットを維持:
                            // RAW_ONLY では recvNano を「保存時刻」として出す（ログ上は recvNano の欄を流用）
                            TimelineLog.logf(
                                    "SUB",
                                    "saved PCD: %s bytes=%d latency_ns=%d latency_ms=%.3f sendNano=%d recvNano=%d",
                                    out.getAbsolutePath(), len, latencyNs, latencyMs, sendNano, saveNano
                            );
                        } else {
                            // sendNano が取れない場合（リング上書き、順序ズレ等）でも最低限ログは出す
                            TimelineLog.logf(
                                    "SUB",
                                    "saved PCD: %s bytes=%d saveNano=%d",
                                    out.getAbsolutePath(), len, saveNano
                            );
                        }

                    } catch (Exception e) {
                        TimelineLog.error("SUB", "failed to handle incoming message", e);
                    }
                });

                active.put(r, sub);
            } catch (Exception e) {
                TimelineLog.error("SUB", "exception while handling incoming message for region=" + r, e);
                TimelineLog.error("SUB", "failed to handle incoming message", e);
            }
        }

        Thread.sleep(stepMs);
        frameIndex++;
    }
}
