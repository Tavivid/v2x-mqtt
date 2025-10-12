v2x-mqtt — クイックスタート & 運用メモ (README.txt)

このリポは MQTT（Mosquitto）を“制御バス”に使い、点群データはブローカを通さず UDP/TCP で直送する想定です。
現在は LiDAR が未接続のため、CSV で指定した要求リージョンを /request で投げるデモ運用をサポートします。

----------------------------------------
■ 構成（ざっくり）
----------------------------------------
- mosquitto …… MQTTブローカ（Docker）
- coordinator … /request を受けて request_id を発行 → /control へ fetch-request を publish
- vehicle …… 
  - Requester: /request を出す／/control を見る
  - Publisher: （将来）点群を直送（いまはデータが無ければ起動しない）
  - CSV feeder（任意）: REQ_FEED_CSV 指定時、CSV の通りに /request を発行

※ 制御トピック（実データは通さない）
  - 依頼:  v2x/region/{region}/request
  - 指示:  v2x/region/{region}/control

----------------------------------------
■ 必要環境
----------------------------------------
- WSL(Ubuntu) or Linux
- Docker / Docker Compose
- JDK 17+, Gradle 8+
- （任意）mosquitto-clients（デバッグ用）
  - sudo apt update && sudo apt install -y mosquitto-clients

----------------------------------------
■ 1. ブローカ（Mosquitto）を起動
----------------------------------------
# リポ直下で
docker compose up -d
docker compose ps
# → mosquitto が 0.0.0.0:1883->1883/tcp で LISTEN していればOK

# 動作確認（コンテナ内から1行だけ受信）
docker exec -it mosquitto sh -lc "mosquitto_sub -h 127.0.0.1 -t '\$SYS/#' -C 1"

----------------------------------------
■ 2. Coordinator を起動
----------------------------------------
# /request を受けて /control に fetch-request を出す実装を前提
./gradlew :coordinator:run

※ 注意：5秒おきに勝手にログが出続けるなら“デモ用の自発ループ版”です。
  /request に反応して /control を publish する実装に差し替えてください。

----------------------------------------
■ 3. Vehicle を起動（CSV フィーダで任意リージョンを要求）
----------------------------------------
[3.1] CSV（要求スケジュール）を用意: requests.csv
# at_ms,region_id,ip,port
0,cell-12,127.0.0.1,51235
800,cell-13,127.0.0.1,51235
1600,cell-12,127.0.0.1,51235

- at_ms: Vehicle 起動時からの相対ミリ秒
- region_id: 依頼対象リージョン
- ip,port: 要求側（受信者）の UDP 宛先（今回はダミーでもOK）

[3.2] Vehicle 起動
# データセットが無ければ Publisher は自動的にスキップされます
REQ_FEED_CSV=./requests.csv ./gradlew :vehicle:run

# 繰り返したい場合
REQ_FEED_CSV=./requests.csv REQ_FEED_LOOP=1 ./gradlew :vehicle:run

起動ログ例
[Vehicle] No dataset -> PublisherTask is not started.
[FEED] started with file=.../requests.csv loop=false

----------------------------------------
■ 4. 動作確認（観察コマンド）
----------------------------------------
# /request を監視（retained 無視）
docker exec -it mosquitto sh -lc \
"mosquitto_sub -h 127.0.0.1 -t 'v2x/region/+/request' -v -R"

# /control を監視（fetch-request を確認）
docker exec -it mosquitto sh -lc \
"mosquitto_sub -h 127.0.0.1 -t 'v2x/region/+/control' -v -R"

期待する流れ：
1) Vehicle のフィーダが /request を CSV 通りに publish
2) Coordinator が受けて request_id/ttl_ms を付与 → /control に fetch-request を publish
3) （将来）Publisher が fetch-request を見て点群を UDP/TCP 直送（現時点ではスキップ）

----------------------------------------
■ 制御メッセージ例
----------------------------------------
[Requester → Coordinator（依頼）]
topic: v2x/region/cell-12/request
{
  "type": "need-pointcloud",
  "region_id": "cell-12",
  "rx_udp": {"ip":"127.0.0.1","port":51235},
  "ts_ms": 1759297000123,
  "nonce": "f0b3-..."
}

[Coordinator → Publisher（取寄せ指示）]
topic: v2x/region/cell-12/control
{
  "type": "fetch-request",
  "region_id": "cell-12",
  "request_id": "7a3f3c2e-....",
  "ttl_ms": 1500,
  "ts_ms": 1759297000456,
  "target": {"ip":"127.0.0.1","port":51235}
}

※ retained=false / QoS0〜1 推奨（過去の指示を残さない）

----------------------------------------
■ よくあるつまずき（チェックリスト）
----------------------------------------
- Coordinator のログが勝手に増える
  → 自発ループ版です。/request に反応する実装に切替。
     msg.isRetained() 無視、厳密トピックマッチ、デバウンス/重複抑止を入れると安定。

- Vehicle が例外で落ちる
  → データセットが無いのに Publisher を起動している可能性。
     現行 VehicleMain はデータ無しなら Publisher をスキップします（最新版を使用）。

- /request は見えるが /control が来ない
  → Coordinator が起動していない／v2x/region/+/request を購読していない／
     出力先が /control になっていない。

- retained に引っ張られる
  → 監視に -R を付ける。アプリ側でも /request の msg.isRetained() を無視。

- clientId の衝突
  → Coordinator/Vehicle ともに prefix + UUID 等で一意化。

----------------------------------------
■ コマンドメモ
----------------------------------------
# 1回だけ手で /request を投げる（WSL）
mosquitto_pub -h 127.0.0.1 -p 1883 \
  -t 'v2x/region/cell-12/request' \
  -m '{"type":"need-pointcloud","region_id":"cell-12","rx_udp":{"ip":"127.0.0.1","port":51235},"ts_ms":1690000000000}'

# 監視（/request）
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'v2x/region/+/request' -v -R

# 監視（/control）
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'v2x/region/+/control' -v -R

----------------------------------------
■ 参考：トピックと役割
----------------------------------------
- v2x/region/{region}/request … 要求（Requester → Coordinator）
- v2x/region/{region}/control … 指示（Coordinator → Publisher）
- v2x/region/{region}/availability … 提供可能アナウンス（Publisher → 全体、任意）
- stats/# … デバッグ用（任意）

----------------------------------------
■ 開発メモ
----------------------------------------
- Vehicle のデフォルト region は -Dregion=... が無ければ「東京駅 GeoHash（精度は設定）」。
- 実データは MQTT を通さない（直送）。いまは CSV で依頼だけ流す。
- セキュリティ：本番は TLS/ACL/匿名禁止 を mosquitto で設定。

----------------------------------------
■ 付録：CSVフィーダの仕様
----------------------------------------
- 環境変数 REQ_FEED_CSV を指定すると Vehicle 起動時にフィーダを有効化
- REQ_FEED_LOOP=1 でファイル末尾まで行ったら先頭から繰り返し
- CSV 形式：at_ms,region_id,ip,port（ヘッダ・空行・#始まりは無視）

以上
