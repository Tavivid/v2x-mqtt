v2x-mqtt — ROS1風 V2X 点群共有デモ (README.txt)

このリポジトリは、ROS1 の pub/sub に似た「自前 Master + TCP 通信」で
LiDAR 点群をリージョン単位で車両間共有するためのプロトタイプです。

- MQTT ブローカは現在は使っていません（mosquitto ディレクトリなどは旧構成の名残）
- Coordinator (= MasterServer) が「ROS master 風」の名前解決を担当
- Vehicle が RosPublisher / RosSubscriber を使って各リージョンの /data トピックで通信
- 実験では、あらかじめ領域ごとに分割済みの PCD データセットを使い、
  JSON タイムラインに従って 10Hz 相当で送受信します

========================================
■ 1. サブプロジェクト構成
========================================
- config/
  - v2x-config.yml … 共通の設定（Vehicle/Coordinator の両方から参照）

- coordinator/
  - v2x.coordinator.MasterServer
    - ROS1 の roscore のような役割
    - registerPublisher / registerSubscriber に相当する処理を担当
  - v2x.coordinator.CoordinatorMain
    - MasterServer を起動する main クラス

- vehicle/
  - v2x.vehicle.VehicleMain … 車両ノードのエントリポイント
  - v2x.vehicle.net.*
    - MasterClient … MasterServer との通信
    - RosPublisher … topic を公開する TCP サーバ
    - RosSubscriber … topic を購読する TCP クライアント
    - Topics … v2x/region/{region}/data などのトピック名ユーティリティ
  - v2x.vehicle.datasource.DatasetPointCloudSource
    - データセットから PointCloudChunk を供給
  - v2x.vehicle.feeder.*
    - PublishFeeder … 旧来の CSV ベース Publish シナリオ
    - SubscribeFeeder … 旧来の CSV ベース Subscribe シナリオ
    - RegionTimelineFeeder … JSON タイムラインに従って Publisher 群を制御
    - RegionTimelineSubscriberFeeder … JSON タイムラインに従って Subscriber 群を制御
  - v2x.vehicle.tasks.*
    - PublisherTask … 単一リージョン固定の簡易 Publisher
    - SubscriberTask … 単一リージョン固定の Subscriber（受信した点群を PCD 保存）

========================================
■ 2. 必要環境
========================================
- WSL(Ubuntu) or Linux
- JDK 17+
- Gradle ラッパー（同梱の ./gradlew を使用）
- LiDAR データセット（後述のディレクトリ構成で配置）
  - ASCII/BINARY PCD もしくは x y z 形式のテキスト

Docker / mosquitto は現在の構成では必須ではありません
（mqtt ブローカは使っていないため）。

========================================
■ 3. ビルド & 共通コマンド
========================================
# 依存解決・ビルドのみ
./gradlew build

# coordinator だけ実行
./gradlew :coordinator:run

# vehicle だけ実行
./gradlew :vehicle:run

========================================
■ 4. Coordinator (MasterServer) の起動
========================================
Coordinator は ROS1 の roscore に相当するコンポーネントです。
Vehicle からの registerPublisher/registerSubscriber 要求を受け、
トピックごとの接続先を教えます。

デフォルトポート: 11311

起動例:
  ./gradlew --no-daemon :coordinator:run --args "11311"

期待ログ例:
  [MASTER] listen on 0.0.0.0:11311
  [MASTER] registerPublisher ... topic=v2x/region/cell-0a3c/data ...
  [MASTER] registerSubscriber ... topic=v2x/region/cell-0a3c/data ...
  ...

========================================
■ 5. データセットとタイムラインの形式
========================================

(1) データセット（点群）の配置

DatasetPointCloudSource は以下のようなディレクトリ構成を前提としています
（datasetPath は v2x-config.yml または環境変数で設定）:

  datasetRoot/
    cell-0a3c/
      000000.pcd
      000001.pcd
      ...
    cell-0a3d/
      000000.pcd
      000001.pcd
      ...

- regionId ごとにサブディレクトリを作成（例: cell-0a3c）
- その中にフレームごとの PCD ファイルを昇順に並べる
- PCD 形式:
  - DATA ascii      -> 1行ごとに x y z ...
  - DATA binary     -> x, y, z の float32 を 12バイトずつ読む簡易実装
  - DATA binary_compressed 等は未対応（スキップ）


(2) Publisher 用 JSON タイムライン

RegionTimelineFeeder は「フレームごとにどのリージョンを持っているか」を
JSON で指定します。ディレクトリ例:

  REGION_TIMELINE_DIR=./timeline/pub-A

  ./timeline/pub-A/
    000000.json
    000001.json
    000002.json
    ...

各 JSON の形式（どちらか）:

  ["cell-0a3c","cell-0a3d"]

または

  { "regions": ["cell-0a3c","cell-0a3d"] }

- フレーム間隔は REGION_TIMELINE_STEP_MS (ms) で指定
  - 例: 100ms -> 10Hz 相当
- そのフレームで regions に含まれているリージョンだけが Publisher になる
- 連続フレームで同じ region が登場する場合、その Publisher は継続し、
  次のフレームでも点群を送信します
- 10点以下のチャンクは Publisher 側で送信スキップします
  （ノイズ的な極小チャンクを避けるため）


(3) Subscriber 用 JSON タイムライン

RegionTimelineSubscriberFeeder は
「ある時刻にどのリージョンを購読したいか」を JSON で指定します:

  SUB_TIMELINE_DIR=./timeline/sub-B

  ./timeline/sub-B/
    000000.json
    000001.json
    ...

形式は Publisher と同じ:

  ["cell-0a3c","cell-0a3d"]
  または
  { "regions": ["cell-0a3c","cell-0a3d"] }

- SUB_TIMELINE_STEP_MS ごとに次の JSON を読み、
  その時点で必要なリージョンの Subscriber を登録/解除します
- Publisher がまだいなくても処理は止まらず、フレームを読み進めます
- 受信した点群は SubscriberTask によって PCD として保存されます

========================================
■ 6. Vehicle の起動モード
========================================

VehicleMain はいくつかのモードを持っています。
現在主に使うのは「JSON タイムラインモード」です。

----------------------------------------
(6-1) JSON タイムライン Publisher モード
----------------------------------------

環境変数:

- MASTER_HOST, MASTER_PORT … Coordinator(MasterServer) の場所
- VEHICLE_ID                 … 車両ID（ログ/メタデータ用）
- REGION_TIMELINE_DIR        … Publisher 用タイムライン JSON ディレクトリ
- REGION_TIMELINE_STEP_MS    … フレーム間隔(ms) 例: 100 (=10Hz)
- REGION_TIMELINE_LOOP       … "1" なら最後まで行ったら先頭に戻る
- DATASET_PATH, DATASET_GLOB, DATASET_LOOP … v2x-config.yml または環境変数

起動例（Publisher 側 Vehicle）:

  MASTER_HOST=127.0.0.1 MASTER_PORT=11311 \
  VEHICLE_ID=vehicle-a \
  REGION_TIMELINE_DIR="./timeline/pub-a" \
  REGION_TIMELINE_STEP_MS=100 \
  REGION_TIMELINE_LOOP=0 \
  ./gradlew --no-daemon :vehicle:run

ログ例:

  [DEBUG] datasetPath=./dataset/k15-44-59 datasetGlob=*.pcd datasetLoop=true
  [Vehicle] id=vehicle-a master=127.0.0.1:11311 ...
  [Timeline] start dir=/.../timeline/pub-a stepMs=100 loop=false
  [Timeline] started publisher region=cell-0a3c host=127.0.0.1 port=51517
  [PUB] frame=0 region=cell-0a3c vehicleId=vehicle-a ts=... points=12345
  ...

※ データセットが見つからない場合は Publisher は自動的にスキップされます。


----------------------------------------
(6-2) JSON タイムライン Subscriber モード
----------------------------------------

環境変数:

- MASTER_HOST, MASTER_PORT … Coordinator の場所
- VEHICLE_ID                 … 車両ID
- SUB_TIMELINE_DIR           … Subscriber 用タイムライン JSON ディレクトリ
- SUB_TIMELINE_STEP_MS       … フレーム間隔(ms)
- SUB_TIMELINE_LOOP          … "1" ならループ

起動例（Subscriber 側 Vehicle）:

  MASTER_HOST=127.0.0.1 MASTER_PORT=11311 \
  VEHICLE_ID=vehicle-b \
  SUB_TIMELINE_DIR="./timeline/sub-b" \
  SUB_TIMELINE_STEP_MS=100 \
  SUB_TIMELINE_LOOP=0 \
  ./gradlew --no-daemon :vehicle:run

ログ例:

  [Vehicle] id=vehicle-b master=127.0.0.1:11311 ...
  [SUB] region timeline started dir=/.../timeline/sub-b stepMs=100 loop=false
  [SUB] subscribe region=cell-0a3c topic=v2x/region/cell-0a3c/data
  [SUB] received chunk region=cell-0a3c vehicleId=vehicle-a ts=... points=12345
  [SUB] saved PCD file: recv-pcd/cell-0a3c/000000.pcd
  ...

受信した点群は以下のように保存されます:

  recv-pcd/
    cell-0a3c/
      000000.pcd   ← 送信元データセットと同じファイル名
      000001.pcd
      ...

送信側が `sourceFileName` を持っていない場合は
`{timestamp}.pcd` のような名前になります。


----------------------------------------
(6-3) CSV モード / 単一リージョンモード（旧仕様）
----------------------------------------

互換のために以下のモードも残っています（README の詳細は省略）。

- CSV Publish モード
  - PUB_FEED_CSV, PUB_FEED_LOOP
  - v2x.vehicle.feeder.PublishFeeder を利用

- CSV Subscribe モード
  - SUB_FEED_CSV, SUB_FEED_LOOP
  - v2x.vehicle.feeder.SubscribeFeeder を利用

- 単一リージョン固定モード
  - pubRegion / SUB_REGION (システムプロパティ or 環境変数)
  - PublisherTask / SubscriberTask を直接起動

現状の実験フローでは JSON タイムラインモードを主に使用します。

========================================
■ 7. 同期起動 (SYNC_START_AT_SEC)
========================================

Publisher/Subscriber を「できるだけ同じ瞬間に」スタートさせたい場合、
環境変数 SYNC_START_AT_SEC を使った簡易バリアを用意しています。

VehicleMain の先頭で waitSyncStartIfConfigured() が呼ばれ、
指定 Unix 時刻(秒)までスリープします。

別ターミナルで、例えば 120 秒後に揃えたい場合:

  SYNC=$(date -d 'now + 120 seconds' +%s)
  echo $SYNC   # この値を控える

Publisher 側:

  MASTER_HOST=127.0.0.1 MASTER_PORT=11311 \
  VEHICLE_ID=vehicle-k \
  REGION_TIMELINE_DIR="/home/tavivid/v2x-pcd/k15-44-59" \
  REGION_TIMELINE_STEP_MS=90 \
  SYNC_START_AT_SEC=1765315118 \
  ./gradlew --no-daemon :vehicle:run

Subscriber 側:

  MASTER_HOST=127.0.0.1 MASTER_PORT=11311 \
  VEHICLE_ID=vehicle-m \
  SUB_TIMELINE_DIR="/home/tavivid/v2x-pcd/subscription/m" \
  SUB_TIMELINE_STEP_MS=100 \
  SYNC_START_AT_SEC=1765315118 \
  ./gradlew --no-daemon :vehicle:run

両者とも指定時刻まで待機し、その後に各スレッドの起動・タイムライン読みが始まります。


========================================
■ 8. よくあるつまずき・チェックリスト
========================================

- Coordinator がすぐに落ちる / スレッド上限
  - 古い実装では「接続ごとにスレッドを無限に増やす」部分がありましたが、
    現在は RosPublisher/RosSubscriber が使い回し・クローズするようになっています。
    それでも pthread_create エラーが出る場合は、
    不要な Publisher/Subscriber が増え続けていないかログを確認してください。

- Subscriber が JSON の最初のフレームで止まる
  - Publisher がまだ居ない時でも RegionTimelineSubscriberFeeder は
    JSON を読み進めるようになっています。
    古いバージョンのクラスを混在させていないか確認してください
    （クリーンビルド推奨: `./gradlew clean build`）。

- 受信点群が保存されない
  - SUB_TIMELINE_* を使わず単一リージョンモードで動かしている場合、
    SubscriberTask の PCD 保存処理が有効になっているか確認する。
  - recv-pcd ディレクトリに書き込み権限があるかチェック。

- Publisher が起動しない
  - datasetPath が存在しない / glob にマッチする PCD が無いケースが多いです。
    ログに `[DATASET]` という prefix で原因が出ます。

- Publisher が早々に「no more data for region=...」
  - そのリージョンの PCD が読み尽くされているか、
    すべての PCD が 10点以下でスキップされている可能性があります。


========================================
■ 9. 設定の上書き（抜粋）
========================================

- v2x-config.yml (config/v2x-config.yml, vehicle/src/main/resources/v2x-config.yml)
  - vehicleId
  - datasetPath / datasetGlob / datasetLoop
  - udpSendPort / udpRecvPort（Publisher のベースポート等）

これらは環境変数や -Dsystem.property で上書き可能です（詳細は AppConfig を参照）。

以上
