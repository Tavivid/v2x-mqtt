package org.example.v2x.vehicle.net;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public final class MqttClientFactory {

    public static MqttClient connect(String host, int port, String baseClientId) throws Exception {
        // 明示指定（MQTT_CLIENT_ID）があれば最優先
        String explicit = System.getenv("MQTT_CLIENT_ID");
        String cid = (explicit != null && !explicit.isBlank())
                ? explicit
                : (baseClientId + "-" + java.util.UUID.randomUUID().toString().substring(0, 8)); // 自動ユニーク化

        String broker = "tcp://" + host + ":" + port;
        MqttClient c = new MqttClient(broker, cid, new MemoryPersistence());
        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(false); // ★再接続で購読を維持/復元
        c.connect(opt);
        System.out.println("[MQTT] connected cid=" + cid + " broker=" + broker);
        return c;
    }
}
