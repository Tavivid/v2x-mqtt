package v2x.vehicle.baseline;

import v2x.vehicle.config.AppConfig;

/**
 * baseline_raw_direct (Pub/Subなし, 領域分割なし, roscore不要)
 *
 * 送信: datasetDir/%06d.pcd を読み、TCPで直送
 * 受信: TCPで受けてファイル保存
 *
 * 環境変数:
 * BASELINE_ROLE=send|recv|both (default: both)
 * BASELINE_DST_HOST (send時に必須)
 * BASELINE_DST_PORT (default: 60000)
 * BASELINE_LISTEN_PORT (default: 60000)
 * BASELINE_DATASET_DIR (default: REGION_TIMELINE_DIR, その次に DATASET_PATH, 最後に ./dataset)
 * BASELINE_RECV_DIR (default: AppConfig.transferRecvDir)
 * BASELINE_STEP_MS (default: REGION_TIMELINE_STEP_MS or 100)
 * BASELINE_LOOP=0|1 (default: 0)
 * BASELINE_TIMELINE_DIR (default: REGION_TIMELINE_DIR) ※jsonがあるなら終端判定に使う
 */
public final class BaselineRawDirectMain {

    public static void main(String[] args) throws Exception {
        AppConfig cfg = AppConfig.load();
        BaselineRawDirectConfig c = BaselineRawDirectConfig.fromEnv(cfg);

        System.out.println("[BaselineRawDirectMain] role=" + c.role);
        System.out.println("[BaselineRawDirectMain] datasetDir=" + c.datasetDir);
        System.out.println("[BaselineRawDirectMain] recvDir=" + c.recvDir);
        System.out.println("[BaselineRawDirectMain] stepMs=" + c.stepMs + " loop=" + c.loop);
        System.out.println("[BaselineRawDirectMain] listenPort=" + c.listenPort + " dst=" + c.dstHost + ":" + c.dstPort);

        BaselineRawDirectReceiver receiver = null;
        Thread senderThread = null;

        // ===== Receiver =====
        if (c.doRecv) {
            receiver = new BaselineRawDirectReceiver(c.listenPort, c.recvDir);
            receiver.start();
        }

        // ===== Sender =====
        if (c.doSend) {
            if (c.dstHost.isBlank()) {
                System.err.println("[BASE-PUB] BASELINE_DST_HOST is empty. (send role requires it)");
            } else {
                BaselineRawDirectSender sender = new BaselineRawDirectSender(
                        c.datasetDir, c.dstHost, c.dstPort, c.stepMs, c.loop, c.timelineDir
                );
                senderThread = new Thread(() -> {
                    try (sender) {
                        sender.run();
                    } catch (Exception e) {
                        System.err.println("[BASE-PUB] failed: " + e.getMessage());
                    }
                }, "baseline-raw-direct-sender");
                senderThread.setDaemon(false);
                senderThread.start();
            }
        }

        // recvのみの場合は待機し続ける
        if (c.doRecv && !c.doSend) {
            System.out.println("[BASE-SUB] waiting forever...");
            while (true) Thread.sleep(60_000);
        }

        // send側が終わったら (bothの場合) recvも閉じて終了
        if (senderThread != null) {
            senderThread.join();
        }
        if (receiver != null) {
            receiver.close();
        }
    }
}
