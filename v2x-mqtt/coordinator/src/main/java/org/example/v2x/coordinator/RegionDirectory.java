package org.example.v2x.coordinator;

import java.time.Instant;
import java.util.*;

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

    public synchronized void announce(String region, String vehicleId, long now) {
        regionToVehicles.computeIfAbsent(region, r -> new HashMap<>())
                .compute(vehicleId, (k, v) -> v == null ? new Entry(vehicleId, now) : (v.lastSeen = now, v)

    
    );
}


public synchronized List<String> candidates(String region, long now) {
        Map<String, Entry> m = regionToVehicles.getOrDefault(region, Collections.emptyMap());
        m.entrySet().removeIf(e -> (now - e.getValue().lastSeen) > ttlMillis);
        return m.values().stream().map(e -> e.vehicleId).toList();
    }
}
