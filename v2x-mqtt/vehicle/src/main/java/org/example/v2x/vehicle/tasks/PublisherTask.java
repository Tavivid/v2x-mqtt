package org.example.v2x.vehicle.tasks;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.example.v2x.common.model.PointCloudChunk;
import org.example.v2x.common.net.DedupCache;
import org.example.v2x.common.net.Topics;
import org.example.v2x.common.util.Jsons;
import org.example.v2x.vehicle.datasource.PointCloudSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class PublisherTask implements Runnable {

    private final MqttClient mqtt;
    private final String vehicleId;
    private final String regionId;
    private final double publishRateHz;
    private final int maxPointsPerChunk;
    private final DedupCache dpd = new DedupCache(2048, Duration.ofSeconds(10));
    private final PointCloudSource source;

    public PublisherTask(MqttClient mqtt, String vehicleId, String regionId,
            double publishRateHz, int maxPointsPerChunk, PointCloudSource source) {
        this.mqtt = mqtt;
        this.vehicleId = vehicleId;
        this.regionId = regionId;
        this.publishRateHz = publishRateHz;
        this.maxPointsPerChunk = maxPointsPerChunk;
        this.source = source;
    }

    @Override
    public void run() {
        long intervalMs = (long) (1000.0 / publishRateHz);
        while (true) {
            try {
// availability アナウンス
                var avail = Jsons.GSON.toJson(java.util.Map.of("vehicleId", vehicleId));
                mqtt.publish(Topics.availability(regionId), new MqttMessage(avail.getBytes(StandardCharsets.UTF_8)));

// データセットから次チャンクを取り出して送信
                PointCloudChunk chunk = source.nextChunk(regionId, vehicleId, maxPointsPerChunk);
                if (chunk == null) {
// データ枯渇（loop=false時）: 少し待って再試行
                    Thread.sleep(1000);
                    continue;
                }
                String key = chunk.makeDedupKey();
                if (!dpd.seen(key, System.currentTimeMillis())) {
                    String json = Jsons.GSON.toJson(chunk);
                    mqtt.publish(Topics.data(regionId), new MqttMessage(json.getBytes(StandardCharsets.UTF_8)));
                }

                Thread.sleep(intervalMs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
