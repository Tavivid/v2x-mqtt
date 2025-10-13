package org.example.v2x.vehicle.tasks;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.example.v2x.common.net.DedupCache;
import org.example.v2x.common.net.Topics;
import org.example.v2x.common.util.Jsons;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * RequesterTask
 *
 * 役割：
 * - ★動的購読用のメッセージ処理を提供（asListener）
 * - 既存の run() は「/request を1回 publish」するレガシー互換。VehicleMain からは呼ばなくてもよい
 *
 * 重要な変更点：
 * - 旧実装のように constructor で mqtt.setCallback()/subscribe() を【しない】
 *   → 初期固定購読を発生させない（購読は DynamicSubscriptionManager 側で行う）
 * - メッセージ処理そのものは従来ロジックを維持（Coordinator 側の見え方/ログの揺れを防ぐ）
 */
public class RequesterTask implements Runnable {

    private final MqttClient mqtt;
    private final String regionId;
    private final DedupCache dpd = new DedupCache(4096, Duration.ofSeconds(10));

    /**
     * 初期購読は行いません。購読は DynamicSubscriptionManager が管理します。
     */
    public RequesterTask(MqttClient mqtt, String regionId) {
        this.mqtt = mqtt;
        this.regionId = regionId;
    }

    /**
     * ★動的購読に差し込むための MQTT リスナーを提供
     * VehicleMain の DynamicSubscriptionManager に渡して使います。
     */
    public IMqttMessageListener asListener() {
        return this::handleIncoming;
    }

    /**
     * 受信メッセージ処理（従来の messageArrived のロジックを移植）
     * Coordinator の fetch-request/points の取り扱いは従来通り。
     */
    public void handleIncoming(String topic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);

        if (payload.contains("\"points\"")) {
            // 点群チャンク（重複排除つき）
            @SuppressWarnings("unchecked")
            Map<String, Object> chunk = Jsons.GSON.fromJson(payload, Map.class);
            String region = (String) chunk.get("regionId");
            String src = (String) chunk.get("sourceVehicleId");
            Double capture = (Double) chunk.get("captureTsMillis");
            long bucket = (capture == null) ? 0L : (long) (capture.longValue() / 1000L);

            String key = region + "|" + src + "|" + bucket;
            if (dpd.seen(key, System.currentTimeMillis())) {
                return; // 重複排除
            }
            System.out.println("[Requester] received points from " + src + " region=" + region);

        } else if (payload.contains("fetch-request")) {
            // Coordinator からの fetch-request（/data に流れてくる制御）
            System.out.println("[Requester] fetch-request seen: " + payload);
        }
    }

    /**
     * 互換：/request を一度だけ publish（必要な場合のみ呼び出し）
     * ※ 初期固定購読はしないため、Topics.data(regionId)の subscribe はここでも行いません。
     */
    @Override
    public void run() {
        try {
            String req = Jsons.GSON.toJson(Map.of(
                "region", regionId,
                "ts", System.currentTimeMillis()
            ));
            mqtt.publish(Topics.request(regionId), new MqttMessage(req.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
