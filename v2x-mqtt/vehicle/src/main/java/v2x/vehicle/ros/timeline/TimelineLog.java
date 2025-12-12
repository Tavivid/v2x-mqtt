package v2x.vehicle.ros.timeline;

public final class TimelineLog {

    private TimelineLog() {}

    private static volatile long baseStartTimeMs = -1L;

    public static void resetElapsedBase() {
        baseStartTimeMs = System.currentTimeMillis();
    }

    private static void initBaseStartTimeIfNeeded() {
        if (baseStartTimeMs < 0) {
            baseStartTimeMs = System.currentTimeMillis();
        }
    }

    public static String elapsed() {
        initBaseStartTimeIfNeeded();
        long ms = System.currentTimeMillis() - baseStartTimeMs;
        long sec = ms / 1000;
        long ms2 = ms % 1000;
        return String.format("[%02d:%02d.%03d]", sec / 60, sec % 60, ms2);
    }

    public static void log(String tag, String msg) {
        System.out.printf("%s [%s] %s%n", elapsed(), tag, msg);
    }

    public static void logf(String tag, String fmt, Object... args) {
        log(tag, String.format(fmt, args));
    }
}
