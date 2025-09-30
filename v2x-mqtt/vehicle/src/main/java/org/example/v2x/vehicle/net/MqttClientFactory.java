package org.example.v2x.vehicle.net;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public final class MqttClientFactory {

    public static MqttClient connect(String host, int port, String clientId) throws Exception {
        MqttClient c = new MqttClient("tcp://" + host + ":" + port, clientId);
        MqttConnectOptions opt = new MqttConnectOptions();
        opt.setAutomaticReconnect(true);
        opt.setCleanSession(true);
        c.connect(opt);
        return c;
    }
}
