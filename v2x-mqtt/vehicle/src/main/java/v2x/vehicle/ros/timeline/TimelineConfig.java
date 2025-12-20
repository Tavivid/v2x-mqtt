package v2x.vehicle.ros.timeline;

import java.io.File;

public final class TimelineConfig {
    private TimelineConfig() {}

    private static long parseLongEnv(String key, long def) {
        try {
            return Long.parseLong(System.getenv().getOrDefault(key, String.valueOf(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static boolean parseBool01Env(String key, boolean def) {
        String v = System.getenv().getOrDefault(key, def ? "1" : "0");
        return "1".equals(v);
    }

    private static File parseDirEnv(String key) {
        String path = System.getenv(key);
        if (path == null || path.isBlank()) return null;
        return new File(path);
    }

    public static final class Pub {
        public final File timelineDir;
        public final long stepMs;
        public final boolean loop;
        public final long syncUnixSec;
        public final PcdPayloadMode payloadMode;

        private Pub(File timelineDir, long stepMs, boolean loop, long syncUnixSec, PcdPayloadMode payloadMode) {
            this.timelineDir = timelineDir;
            this.stepMs = stepMs;
            this.loop = loop;
            this.syncUnixSec = syncUnixSec;
            this.payloadMode = payloadMode;
        }

        public static Pub fromEnv() {
            File dir = parseDirEnv("REGION_TIMELINE_DIR");
            long step = parseLongEnv("REGION_TIMELINE_STEP_MS", 100L);
            boolean loop = parseBool01Env("REGION_TIMELINE_LOOP", false);
            long sync = parseLongEnv("TIMELINE_SYNC_UNIX", 0L);
            PcdPayloadMode mode = PcdPayloadMode.fromEnv();
            return new Pub(dir, step, loop, sync, mode);
        }
    }

    public static final class Sub {
        public final File timelineDir;
        public final long stepMs;
        public final boolean loop;
        public final long syncUnixSec;
        public final PcdPayloadMode payloadMode;

        private Sub(File timelineDir, long stepMs, boolean loop, long syncUnixSec, PcdPayloadMode payloadMode) {
            this.timelineDir = timelineDir;
            this.stepMs = stepMs;
            this.loop = loop;
            this.syncUnixSec = syncUnixSec;
            this.payloadMode = payloadMode;
        }

        public static Sub fromEnv() {
            File dir = parseDirEnv("SUB_TIMELINE_DIR");
            long step = parseLongEnv("SUB_TIMELINE_STEP_MS", 100L);
            boolean loop = parseBool01Env("SUB_TIMELINE_LOOP", false);
            long sync = parseLongEnv("TIMELINE_SYNC_UNIX", 0L);
            PcdPayloadMode mode = PcdPayloadMode.fromEnv();
            return new Sub(dir, step, loop, sync, mode);
        }
    }
}
