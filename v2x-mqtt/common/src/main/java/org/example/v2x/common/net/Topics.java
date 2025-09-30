package org.example.v2x.common.net;

/**
 * MQTTトピック規約。
 */
public final class Topics {

    public static String availability(String region) {
        return "v2x/region/" + region + "/availability";
    }

    public static String request(String region) {
        return "v2x/region/" + region + "/request";
    }

    public static String data(String region) {
        return "v2x/region/" + region + "/data";
    }

    public static String udpAnnounce() {
        return "v2x/udp/announce";
    }

    private Topics() {
    }
}
