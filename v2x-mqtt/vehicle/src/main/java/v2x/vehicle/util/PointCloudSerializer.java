package v2x.vehicle.util;

import v2x.vehicle.model.PointCloudChunk;

import java.nio.charset.StandardCharsets;

/**
 * PointCloudChunk のシリアライズ／デシリアライズ。
 *
 * Jsons.GSON を使って JSON 文字列 ⇔ byte[] に変換する。
 */
public final class PointCloudSerializer {

    private PointCloudSerializer() {
    }

    public static byte[] serialize(String vehicleId, String regionId, PointCloudChunk chunk) {
        // 今は chunk だけをシリアライズしている。
        // vehicleId / regionId も一緒に送りたくなったらラッパー DTO を定義してそこに詰めても良い。
        String json = Jsons.GSON.toJson(chunk);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    public static PointCloudChunk deserialize(byte[] payload) {
        String json = new String(payload, StandardCharsets.UTF_8);
        return Jsons.GSON.fromJson(json, PointCloudChunk.class);
    }
}
