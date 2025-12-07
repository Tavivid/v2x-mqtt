package v2x.coordinator;

public class CoordinatorMain {

    public static void main(String[] args) {
        int port = 11311; // デフォルト master ポート
        if (args.length >= 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException ignore) {
                System.err.println("[Master] invalid port '" + args[0] + "', use default " + port);
            }
        }

        MasterServer master = new MasterServer(port);
        master.run(); // ブロッキングで実行
    }
}
