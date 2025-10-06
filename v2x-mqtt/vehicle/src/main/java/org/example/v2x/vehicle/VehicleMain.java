package org.example.v2x.vehicle;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.example.v2x.common.config.AppConfig;
import org.example.v2x.common.geo.GeoHash;
import org.example.v2x.vehicle.datasource.DatasetPointCloudSource;
import org.example.v2x.vehicle.datasource.PointCloudSource;
import org.example.v2x.vehicle.net.MqttClientFactory;
import org.example.v2x.vehicle.tasks.PublisherTask;
import org.example.v2x.vehicle.tasks.RequesterTask;

import java.io.InputStream;

public class VehicleMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();

// 位置情報があるならここで緯度経度からGeoHashを生成（デモでは固定）
        String region = System.getProperty("region", null);
        if (region == null || region.isBlank()) {
// 例: 東京駅付近
            region = GeoHash.encode(35.681236, 139.767125, cfg.geohashPrecision);
        }

        MqttClient client = MqttClientFactory.connect(cfg.mqttHost, cfg.mqttPort, cfg.mqttClientPrefix + cfg.vehicleId);

// データセットソースの準備
        PointCloudSource source = new DatasetPointCloudSource(cfg.datasetPath, cfg.datasetGlob, cfg.datasetLoop);

// パブリッシャ（データセット由来の点群発行）
        new Thread(new PublisherTask(client, cfg.vehicleId, region, cfg.publishRateHz, cfg.maxPointsPerChunk, source),
                "Publisher").start();

// リクエスタ（必要なリージョンのデータ要求）
        final var regionFinal = region;
        final var clientFinal = client;

        new Thread(() -> {
            try {
                new RequesterTask(clientFinal, regionFinal).run();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "Requester").start();

        System.out.println("Vehicle started for region=" + region + ", dataset=" + cfg.datasetPath);

// 設定ファイルの存在確認（Shade/Jarに埋め込む前提）
        try (InputStream in = VehicleMain.class.getResourceAsStream("/v2x-config.yml")) {
            if (in == null) {
                System.err.println("WARNING: v2x-config.yml not found in classpath!");
            }
        }
    }
}
