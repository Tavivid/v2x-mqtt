package v2x.vehicle.baseline;

import v2x.vehicle.config.AppConfig;

import java.io.File;

public final class BaselineRawDirectConfig {
    public final String role;
    public final boolean doSend;
    public final boolean doRecv;

    public final String dstHost;
    public final int dstPort;
    public final int listenPort;

    public final String datasetDir;
    public final String recvDir;

    public final long stepMs;
    public final boolean loop;

    public final File timelineDir;

    private BaselineRawDirectConfig(
            String role,
            boolean doSend,
            boolean doRecv,
            String dstHost,
            int dstPort,
            int listenPort,
            String datasetDir,
            String recvDir,
            long stepMs,
            boolean loop,
            File timelineDir
    ) {
        this.role = role;
        this.doSend = doSend;
        this.doRecv = doRecv;
        this.dstHost = dstHost;
        this.dstPort = dstPort;
        this.listenPort = listenPort;
        this.datasetDir = datasetDir;
        this.recvDir = recvDir;
        this.stepMs = stepMs;
        this.loop = loop;
        this.timelineDir = timelineDir;
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null) ? def : v;
    }

    private static int envInt(String k, int def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static long envLong(String k, long def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static boolean envBool01(String k, boolean def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return def;
        return "1".equals(v.trim());
    }

    private static File envDir(String k) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return null;
        return new File(v.trim());
    }

    public static BaselineRawDirectConfig fromEnv(AppConfig cfg) {
        String role = env("BASELINE_ROLE", "both").trim().toLowerCase();
        boolean doSend = role.equals("send") || role.equals("both");
        boolean doRecv = role.equals("recv") || role.equals("both");

        String dstHost = env("BASELINE_DST_HOST", "").trim();
        int dstPort = envInt("BASELINE_DST_PORT", 60000);
        int listenPort = envInt("BASELINE_LISTEN_PORT", 60000);

        // datasetDir: BASELINE_DATASET_DIR > REGION_TIMELINE_DIR > DATASET_PATH > ./dataset
        String datasetDir = env("BASELINE_DATASET_DIR", "").trim();
        if (datasetDir.isBlank()) datasetDir = env("REGION_TIMELINE_DIR", "").trim();
        if (datasetDir.isBlank()) datasetDir = env("DATASET_PATH", cfg.datasetPath).trim();
        if (datasetDir.isBlank()) datasetDir = "./dataset";

        String recvDir = env("BASELINE_RECV_DIR", cfg.transferRecvDir).trim();

        long stepMs = envLong("BASELINE_STEP_MS", envLong("REGION_TIMELINE_STEP_MS", 100L));
        boolean loop = envBool01("BASELINE_LOOP", false);

        // 終端判定の目安：jsonがあるなら frames 数として使う（内容は無視）
        File timelineDir = envDir("BASELINE_TIMELINE_DIR");
        if (timelineDir == null) {
            timelineDir = envDir("REGION_TIMELINE_DIR");
        }

        return new BaselineRawDirectConfig(
                role, doSend, doRecv, dstHost, dstPort, listenPort,
                datasetDir, recvDir, stepMs, loop, timelineDir
        );
    }
}
