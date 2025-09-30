package org.example.v2x.coordinator;

import org.eclipse.paho.client.mqttv3.*;
import org.example.v2x.common.config.AppConfig;
import org.example.v2x.common.net.Topics;
import org.example.v2x.common.util.Jsons;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class SubscriptionManager implements MqttCallback {

    private final AppConfig cfg;
    private final MqttClient client;
    private final RegionDirectory dir;

    public SubscriptionManager(AppConfig cfg, RegionDirectory dir) throws Exception {
        this.cfg = cfg;
        this.dir = dir;
        this.client = new MqttClient("tcp://" + cfg.mqttHost + ":" + cfg.mqttPort, cfg.mqttClientPrefix + "coordinator");
        this.client.setCallback(this);
        this.client.connect();
        this.client.subscribe("v2x/region/+/availability");
        this.client.subscribe("v2x/region/+/request");
    }

    @Override
    public void connectionLost(Throwable cause) {
        cause.printStackTrace();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();
        if (topic.contains("/availability")) {
// payload: {"vehicleId":"vehicle-A"}
            var obj = Jsons.GSON.fromJson(payload, java.util.Map.class);
            String vehicleId = (String) obj.get("vehicleId");
            String region = topic.split("/")[2];
            dir.announce(region, vehicleId, now);
        } else if (topic.contains("/request")) {
            String region = topic.split("/")[2];
            List<String> cands = dir.candidates(region, now);
// Coordinatorはシンプルにブロードキャスト（対象リージョンのdataトピックへ通知）
            try {
                String announce = Jsons.GSON.toJson(java.util.Map.of(
                        "type", "fetch-request",
                        "region", region,
                        "requestTs", now
                ));
                client.publish(Topics.data(region), new MqttMessage(announce.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
