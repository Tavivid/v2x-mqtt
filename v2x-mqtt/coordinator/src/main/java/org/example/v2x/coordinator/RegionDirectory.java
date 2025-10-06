package org.example.v2x.coordinator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 各リージョンに対して提供可能な車両の最新アナウンスを保持。
 */
public class RegionDirectory {

    public static class Entry {
        public final String vehicleId;
        public volatile long lastSeen;

        public Entry(String vehicleId, long lastSeen) {
            this.vehicleId = vehicleId;
            this.lastSeen = lastSeen;
        }
    }

    private final Map<String, Map<String, Entry>> regionToVehicles = new HashMap<>();
    private final long ttlMillis;

    public RegionDirectory(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    /** リージョンと車両IDのアナウンスを登録/更新 */
    public synchronized void announce(String region, String vehicleId, long now) {
        Map<String, Entry> veh = regionToVehicles.computeIfAbsent(region, r -> new HashMap<>());
        veh.compute(vehicleId, (k, v) -> {
            if (v == null) {
                return new Entry(vehicleId, now);
            }
            v.lastSeen = now;
            return v;
        });
    }

    /** 有効期限内の候補車両IDを返す（古いものは掃除） */
    public synchronized List<String> candidates(String region, long now) {
        Map<String, Entry> m = regionToVehicles.get(region);
        if (m == null) return Collections.emptyList();
        m.entrySet().removeIf(e -> (now - e.getValue().lastSeen) > ttlMillis);
        return m.values().stream().map(e -> e.vehicleId).collect(Collectors.toList());
    }
}
