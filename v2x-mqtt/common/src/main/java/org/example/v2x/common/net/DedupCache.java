package org.example.v2x.common.net;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 簡易LRU + TTL。DPD用。
 */
public class DedupCache {

    private final int maxEntries;
    private final long ttlMillis;
    private final Map<String, Long> map;

    public DedupCache(int maxEntries, Duration ttl) {
        this.maxEntries = maxEntries;
        this.ttlMillis = ttl.toMillis();
        this.map = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
                return size() > DedupCache.this.maxEntries;
            }
        };
    }

    public synchronized boolean seen(String key, long nowMillis) {
// true => 既に見た
        Long t = map.get(key);
        if (t != null && (nowMillis - t) <= ttlMillis) {
            return true;
        }
        map.put(key, nowMillis);
// TTL切れの掃除（雑だが軽量）
        map.entrySet().removeIf(e -> (nowMillis - e.getValue()) > ttlMillis);
        return false;
    }
}
