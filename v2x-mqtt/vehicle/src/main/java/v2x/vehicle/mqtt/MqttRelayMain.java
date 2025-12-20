package v2x.vehicle.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import v2x.vehicle.config.AppConfig;
import v2x.vehicle.ros.timeline.TimelineLog;

import java.util.UUID;

/**
 * MQTT relay (no roscore) - PublisherTimelineLoop 相当: MqttPublisherTimelineLoop
 * - SubscriberTimelineLoop 相当: MqttSubscriberTimelineLoop
 *
 * 環境変数: MQTT_HOST / MQTT_PORT MQTT_QOS (default 0) ※比較実験で QoS=1 にしたい場合は 1
 * MQTT_MAX_INFLIGHT (default 1000)
 *
 * REGION_TIMELINE_DIR / REGION_TIMELINE_STEP_MS / REGION_TIMELINE_LOOP(0|1)
 * SUB_TIMELINE_DIR / SUB_TIMELINE_STEP_MS / SUB_TIMELINE_LOOP(0|1)
 *
 * MQTT_RELAY_RECV_DIR (default /home/tavivid/v2x-pcd/received_raw_mqttrelay)
 * MQTT_SAVE_QUEUE (default 4096) MQTT_SAVE_THREADS (default 1)
 */
public final class MqttRelayMain {

    public static void main(String[] args) throws Exception {
        final AppConfig cfg = AppConfig.load();

        final String brokerHost = Env.env("MQTT_HOST", cfg.mqttHost);
        final int brokerPort = Env.envInt("MQTT_PORT", cfg.mqttPort);
        final String brokerUri = "tcp://" + brokerHost + ":" + brokerPort;

        final int qos = Env.envInt("MQTT_QOS", 0);
        final int maxInflight = Env.envInt("MQTT_MAX_INFLIGHT", 1000);

        final String clientId = cfg.mqttClientPrefix + cfg.vehicleId + "-mqttrelay-" + UUID.randomUUID();

        final MqttClient client = new MqttClient(brokerUri, clientId);
        final MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);
        opt.setMaxInflight(maxInflight);

        client.connect(opt);

        TimelineLog.logf("MQTT-RELAY", "connected broker=%s clientId=%s qos=%d maxInflight=%d",
                brokerUri, clientId, qos, maxInflight);

        // ---- Subscriber loop ----
        final MqttSubscriberTimelineLoop subLoop
                = new MqttSubscriberTimelineLoop(client, qos);

        Thread subThread = null;
        if (subLoop.isEnabled()) {
            subThread = new Thread(subLoop::runForever, "MqttSubscriberTimelineLoop");
            subThread.setDaemon(true);
            subThread.start();
        } else {
            TimelineLog.log("MQTT-RELAY", "subscriber disabled (SUB_TIMELINE_DIR is empty or invalid)");
        }

        // ---- Publisher loop ----
        final MqttPublisherTimelineLoop pubLoop
                = new MqttPublisherTimelineLoop(client, cfg, qos);

        Thread pubThread = null;
        if (pubLoop.isEnabled()) {
            pubThread = new Thread(pubLoop::runForever, "MqttPublisherTimelineLoop");
            pubThread.setDaemon(true);
            pubThread.start();
        } else {
            TimelineLog.log("MQTT-RELAY", "publisher disabled (REGION_TIMELINE_DIR is empty or invalid)");
        }

        // メインスレッドは待機
        if (pubThread != null) {
            pubThread.join();
        }
        if (subThread != null) {
            subThread.join();
        }

        client.disconnect();
        client.close();
    }

    /**
     * Env helpers（rosjava側と同じように env を中心に動かす）
     */
    static final class Env {

        static String env(String k, String def) {
            String v = System.getenv(k);
            return (v == null || v.isBlank()) ? def : v;
        }

        static int envInt(String k, int def) {
            String v = System.getenv(k);
            if (v == null || v.isBlank()) {
                return def;
            }
            try {
                return Integer.parseInt(v.trim());
            } catch (NumberFormatException e) {
                return def;
            }
        }

        static long envLong(String k, long def) {
            String v = System.getenv(k);
            if (v == null || v.isBlank()) {
                return def;
            }
            try {
                return Long.parseLong(v.trim());
            } catch (NumberFormatException e) {
                return def;
            }
        }

        static boolean envBool01(String k, boolean def) {
            String v = System.getenv(k);
            if (v == null || v.isBlank()) {
                return def;
            }
            v = v.trim();
            if ("1".equals(v) || "true".equalsIgnoreCase(v)) {
                return true;
            }
            if ("0".equals(v) || "false".equalsIgnoreCase(v)) {
                return false;
            }
            return def;
        }
    }
}
