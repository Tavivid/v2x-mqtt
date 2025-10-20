v2x-mqtt — クイックスタート & 運用メモ (README.txt)

このリポは MQTT（Mosquitto）を“制御バス”に使い、点群データはブローカを通さず UDP/TCP で直送する想定です。
現在は LiDAR が未接続のため、CSV で指定した要求リージョンを /request で投げるデモ運用をサポートします。

----------------------------------------
■ 構成（ざっくり）
----------------------------------------
- mosquitto …… MQTTブローカ（Docker）
- coordinator … /request を受けて request_id を発行 → /control へ fetch-request を publish
- vehicle ……
  - Request feeder: REQ_FEED_CSV に従って /request を publish（QoS 推奨: 1）
  - Dynamic subscribe: SUB_FEED_CSV に従って /data を動的購読（アイドルで自動解除）
  - （任意）Publisher: データセットがあれば点群を直送（未接続なら起動しない）

※ 制御トピック（実データは通さない）
  - 依頼:  v2x/region/{region}/request
  - 指示:  v2x/region/{region}/data

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

起動後の期待ログ例
[COORD] connected ...
[COORD] (re)subscribed: v2x/region/+/request qos=1
# /request を受信すると
[COORD][RX] topic=.../request ...
[COORD][TX] -> .../data qos=1 payload={"type":"fetch-request", ...}
[COORD] publish fetch-request -> topic=.../data {...}

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

[3.2] 「購読CSV」（必要な場合のみ）: subscribe.csv
# at_ms,region_id
0,cell-10
500,cell-12
1000,cell-14

- 指定時刻で /data 購読を開始（SUB_IDLE_MS 経過後は自動で unsubscribe）
- 受信処理は RequesterTask を流用（動的購読のリスナーとして使用）

[3.3] Vehicle 起動
# データセットが無ければ Publisher は自動スキップ
# VEHICLE_ID は AppConfig の YAML を環境変数で上書き可能（重複起動時は必ず変えるか ClientID をユニーク化）
# 購読者両側
VEHICLE_ID=vehA \
REQ_FEED_CSV="requests.csv" \
REQ_FEED_LOOP=1 \
./gradlew --no-daemon :vehicle:run

# 発行者両側
VEHICLE_ID=vehB \
PUB_FEED_CSV="subscribe.csv" \
PUB_FEED_LOOP=0 \
./gradlew --no-daemon :vehicle:run

ログ例
[Vehicle] Dataset not available: Dataset path not found: ./dataset
[Vehicle] No dataset -> PublisherTask is not started.
Vehicle started (dynamic-subscribe only; handler=RequesterTask) defaultRegion=xn76urx6, dataset=./dataset
[FEED-REQ] started file=.../requests.csv loop=false
[FEED-SUB] started file=.../subscribe.csv loop=false
[DYN] subscribed v2x/region/cell-10/data
[FEED-REQ] published topic=v2x/region/cell-10/request payload={...}
[Requester] fetch-request seen: {...}
...

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
■ 設定と上書き（抜粋）
----------------------------------------
- AppConfig（YAML）を環境変数/システムプロパティで上書き可能
  - VEHICLE_ID / -DvehicleId … 車両ID（ClientIDではない）。複数起動時はユニークに
  - MQTT_HOST / -Dmqtt.host … ブローカホスト
  - MQTT_PORT / -Dmqtt.port … ブローカポート
  - MQTT_CLIENT_PREFIX / -Dmqtt.clientPrefix … ClientID のベース
  - DATASET_PATH / DATASET_LOOP / DATASET_GLOB … データセット関連（無ければ Publisher は起動しない）

- ClientID の衝突回避（どれか1つ）
  1) VEHICLE_ID をインスタンスごとに変える
  2) `MQTT_CLIENT_ID` を個別指定（例：vehA-01, vehB-01）
  3) MqttClientFactory で `prefix + vehicleId + "-{短いUUID}"` を自動付与（推奨）
     併せて `AutomaticReconnect=true`, `CleanSession=false` を設定

----------------------------------------
■ QoS/再接続の推奨
----------------------------------------
- Vehicle の /request publish …… QoS=1（瞬断時の取りこぼしを減らす）
- Coordinator の /request subscribe …… QoS=1
- Coordinator の /data publish …… QoS=1
- すべてのクライアントで `AutomaticReconnect=true`, `CleanSession=false` を推奨
- 再接続時は購読の明示再登録（MqttCallbackExtended#connectComplete）を保険として実装

----------------------------------------
■ 制御メッセージ例
----------------------------------------
[Requester → Coordinator（依頼）]
topic: v2x/region/cell-12/request
{
  "type": "need-pointcloud",
  "region_id": "cell-12",
  "rx_udp": {"ip":"127.0.0.1","port":51235},
  "ts_ms": 1760385543335,
  "nonce": "95ebdbf3-..."
}

[Coordinator → Vehicle/Pub側（取寄せ指示）]
topic: v2x/region/cell-12/data
{
  "type": "fetch-request",
  "region_id": "cell-12",
  "request_id": "5c1f78ab-...",
  "ttl_ms": 1500,
  "ts_ms": 1760385543344
}

※ retained=false。購読開始より前に publish されたものは受け取れません。）

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
