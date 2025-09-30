package org.example.v2x.vehicle.tasks;

import org.eclipse.paho.client.mqttv3.*;
import org.example.v2x.common.net.DedupCache;
import org.example.v2x.common.net.Topics;
import org.example.v2x.common.util.Jsons;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class RequesterTask implements Runnable, MqttCallback {

    private final MqttClient mqtt;
    private final String regionId;
    private final DedupCache dpd = new DedupCache(4096, Duration.ofSeconds(10));

    public RequesterTask(MqttClient mqtt, String regionId) throws Exception {
        this.mqtt = mqtt;
        this.regionId = regionId;
        mqtt.setCallback(this);
        mqtt.subscribe(Topics.data(regionId));
    }

    @Override
    public void run() {
        try {
            String req = Jsons.GSON.toJson(java.util.Map.of("region", regionId, "ts", System.currentTimeMillis()));
            mqtt.publish(Topics.request(regionId), new MqttMessage(req.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
        if (payload.contains("\"points\"")) {
// 点群チャンク
            var chunk = Jsons.GSON.fromJson(payload, java.util.Map.class);
            String region = (String) chunk.get("regionId");
            String src = (String) chunk.get("sourceVehicleId");
            Double capture = (Double) chunk.get("captureTsMillis");
            String key = region + "|" + src + "|" + (capture.longValue() / 1000L);
            if (dpd.seen(key, System.currentTimeMillis())) {
                return; // 重複排除

                        }System.out.println("[Requester] received points from " + src + " region=" + region);
        } else if (payload.contains("fetch-request")) {
            System.out.println("[Requester] fetch-request seen: " + payload);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
