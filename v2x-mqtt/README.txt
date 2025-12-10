v2x-mqtt — rosjava / ROS1 V2X 点群共有デモ (README.txt)

このリポジトリは、ROS1 (roscore) + rosjava を使った
「リージョン単位の LiDAR 点群共有」のプロトタイプです。

- MQTT ブローカは現在は使っていません（mosquitto ディレクトリなどは旧構成の名残）
- 独自の MasterServer は使わず、標準の ROS master (roscore) を利用します
- Vehicle は rosjava ノードとして /v2x/region/{regionId}/data トピックで通信します
- 実験では、あらかじめ領域ごとに分割済みの PCD データセットを用意し、
  JSON タイムラインに従って 10Hz 相当で送受信します
- 必要に応じて、UNIX 時刻を使った「起動同期 (TIMELINE_SYNC_UNIX)」も利用できます


========================================
■ 1. サブプロジェクト構成
========================================
- config/
  - v2x-config.yml … 共通の設定（Vehicle から参照）

- vehicle/
  - v2x.vehicle.VehicleMain
    - 旧構成のエントリポイント（独自 MasterServer + TCP 用）
    - rosjava 版では基本的に使いません（互換用に残してあります）
  - v2x.vehicle.ros.*
    - VehicleRosMain … rosjava ノードのエントリポイント（:vehicle:runRos で起動）
    - VehicleTimelineNode … JSON タイムラインに従って
      /v2x/region/{regionId}/data を publish / subscribe する NodeMain
  - v2x.vehicle.net.*
    - Topics … v2x/region/{regionId}/data などのトピック名ユーティリティ
    - RosPublisher / RosSubscriber / MasterClient などは旧実装用クラス
      （rosjava 版では基本的には使用しない）
  - v2x.vehicle.datasource.DatasetPointCloudSource
    - データセットから PointCloudChunk を供給
  - v2x.vehicle.tasks.*
    - PublisherTask / SubscriberTask など、旧構成での簡易タスク
      （rosjava タイムライン版では直接は使いません）
  - v2x.vehicle.util.*
    - PointCloudSerializer … PointCloudChunk を ByteMultiArray にシリアライズ
    - Jsons … Gson ラッパユーティリティ

- coordinator/
  - v2x.coordinator.MasterServer / CoordinatorMain
    - 独自 Master + TCP 版のためのコンポーネント
    - rosjava / roscore を使う現行構成では通常は使用しません
    - 過去構成の参考用として残しています


========================================
■ 2. 必要環境
========================================
- WSL(Ubuntu) or Linux
- ROS1 (例: noetic) がインストールされていること
  - roscore コマンドが使えること
- rosjava 関連の依存（本リポジトリの Gradle から取得されます）
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

# rosjava Vehicle ノードのみ実行（ROS1 用）
./gradlew :vehicle:runRos

※ いずれも ROS の設定（ROS_MASTER_URI, ROS_IP）は
  実行シェルの環境変数として指定してください。


========================================
■ 4. ROS master (roscore) の起動
========================================
rosjava 版では、独自の Coordinator / MasterServer は使わず、
標準の roscore を利用します。

別ターミナルで roscore を起動しておきます:

  roscore

その上で、Vehicle 側では例えば以下のように環境変数を設定します:

  export ROS_MASTER_URI=http://127.0.0.1:11311
  export ROS_IP=127.0.0.1

※ WSL / Docker 等でホストと疎通する際は、
  適切な IP アドレス (例: 192.168.x.x など) を ROS_IP に指定してください。


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

VehicleTimelineNode は「フレームごとにどのリージョンを持っているか」を
JSON で指定します。ディレクトリ例:

  REGION_TIMELINE_DIR=/home/tavivid/v2x-pcd/k15-44-59

  /home/tavivid/v2x-pcd/k15-44-59/
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
- そのフレームで regions に含まれているリージョンだけが Publisher として有効になり、
  各リージョンについて /v2x/region/{regionId}/data に ByteMultiArray を publish します
- 連続フレームで同じ region が登場する場合、その Publisher は継続し、
  次のフレームでも点群を送信します
- 10点以下のチャンクは Publisher 側で送信スキップします
  （ノイズ的な極小チャンクを避けるため）


(3) Subscriber 用 JSON タイムライン

同じく VehicleTimelineNode は
「ある時刻にどのリージョンを購読したいか」を JSON で指定します:

  SUB_TIMELINE_DIR=/home/tavivid/v2x-pcd/subscription/vehicle-m

  /home/tavivid/v2x-pcd/subscription/vehicle-m/
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
- 受信した点群は VehicleTimelineNode 内の saveChunkAsPcd() により
  /home/tavivid/v2x-pcd/received/{regionId}/{fileName}.pcd に保存されます


========================================
■ 6. Vehicle（rosjava）の起動モード
========================================

rosjava 版では、VehicleRosMain / VehicleTimelineNode を使って、
1 プロセスで Publisher と Subscriber の両方を扱うことも、
どちらか片方のみを有効にすることもできます。

エントリポイント:
  - :vehicle:runRos  → VehicleRosMain を起動

基本の環境変数:

- ROS_MASTER_URI        … roscore の URI (例: http://127.0.0.1:11311)
- ROS_IP                … この Vehicle の IP
- VEHICLE_ID            … 車両ID（ログ/メタデータ用）

Publisher 用:

- REGION_TIMELINE_DIR   … Publisher 用タイムライン JSON ディレクトリ
- REGION_TIMELINE_STEP_MS … フレーム間隔(ms) 例: 100 (=10Hz)
- REGION_TIMELINE_LOOP  … "1" なら最後まで行ったら先頭に戻る（周回）
                           "0" または未設定なら 1 周して停止

Subscriber 用:

- SUB_TIMELINE_DIR      … Subscriber 用タイムライン JSON ディレクトリ
- SUB_TIMELINE_STEP_MS  … フレーム間隔(ms)
- SUB_TIMELINE_LOOP     … "1" ならループ、"0" または未設定なら 1 周して停止

データセット:

- DATASET_PATH / DATASET_GLOB / DATASET_LOOP
  - v2x-config.yml に設定があればそちらが優先されます
  - さらに環境変数で上書き可能（詳細は AppConfig を参照）


----------------------------------------
(6-1) JSON タイムライン Publisher モード（rosjava）
----------------------------------------

Publisher だけを動かしたい場合は、REGION_TIMELINE_* を設定し、
SUB_TIMELINE_DIR を設定しない、または存在しないディレクトリにしておきます。

起動例（Publisher 側 Vehicle）:

  export ROS_MASTER_URI=http://127.0.0.1:11311
  export ROS_IP=127.0.0.1

  VEHICLE_ID=vehicle-k \
  REGION_TIMELINE_DIR="/home/tavivid/v2x-pcd/k15-44-59" \
  REGION_TIMELINE_STEP_MS=100 \
  REGION_TIMELINE_LOOP=0 \
  ./gradlew --no-daemon :vehicle:runRos

ログ例:

  [00:00.000] [PUB-TL] started timeline dir=/home/tavivid/v2x-pcd/k15-44-59 stepMs=100 loop=false syncUnix=0
  [00:00.001] [PUB-TL] reading timeline frame=0 file=000000.json
  [00:00.001] [PUB-TL] region active: cell-0a3c
  [00:00.010] [PUB-TL] reading timeline frame=1 file=000001.json
  ...

※ データセットが見つからない場合は Publisher は自動的にスキップされます。
※ REGION_TIMELINE_LOOP=1 の場合は、最後まで行った後に 0 フレームへ戻り、周回します。


----------------------------------------
(6-2) JSON タイムライン Subscriber モード（rosjava）
----------------------------------------

Subscriber だけを動かしたい場合は、SUB_TIMELINE_* を設定し、
REGION_TIMELINE_DIR を設定しない、または存在しないディレクトリにしておきます。

起動例（Subscriber 側 Vehicle）:

  export ROS_MASTER_URI=http://127.0.0.1:11311
  export ROS_IP=127.0.0.1

  VEHICLE_ID=vehicle-m \
  SUB_TIMELINE_DIR="/home/tavivid/v2x-pcd/subscription/m" \
  SUB_TIMELINE_STEP_MS=100 \
  SUB_TIMELINE_LOOP=0 \
  ./gradlew --no-daemon :vehicle:runRos

ログ例:

  [00:00.000] [SUB-TL] started timeline dir=/home/tavivid/v2x-pcd/subscription/m stepMs=100 loop=false syncUnix=0
  [00:00.001] [SUB] received chunk region=cell-0a3c vehicleId=vehicle-k ts=... points=12345
  [00:00.002] [SUB] saved PCD file: /home/tavivid/v2x-pcd/received/cell-0a3c/000000.pcd
  ...

受信した点群は以下のように保存されます:

  /home/tavivid/v2x-pcd/received/
    cell-0a3c/
      000000.pcd   ← 送信元データセットと同じファイル名
      000001.pcd
      ...

送信側が sourceFileName を持っていない場合は
`{timestamp}.pcd` のような名前になります。


----------------------------------------
(6-3) 旧 CSV モード / 単一リージョンモード（互換用）
----------------------------------------

- CSV Publish モード
  - PUB_FEED_CSV, PUB_FEED_LOOP
  - v2x.vehicle.feeder.PublishFeeder を利用（VehicleMain 経由）

- CSV Subscribe モード
  - SUB_FEED_CSV, SUB_FEED_LOOP
  - v2x.vehicle.feeder.SubscribeFeeder を利用

- 単一リージョン固定モード
  - pubRegion / SUB_REGION (システムプロパティ or 環境変数)
  - PublisherTask / SubscriberTask を直接起動

これらは独自 Master + TCP を使う旧構成用であり、
rosjava / roscore を使う現行の実験フローでは
基本的には JSON タイムライン + VehicleTimelineNode を用います。


========================================
■ 7. 同期起動 (TIMELINE_SYNC_UNIX)
========================================

複数の Vehicle を「できるだけ同じ瞬間に」タイムライン開始させたい場合、
UNIX 時刻(秒)で指定する環境変数 TIMELINE_SYNC_UNIX を使った簡易バリアがあります。

VehicleTimelineNode 内で、PublisherTimelineLoop / SubscriberTimelineLoop の
各ループがその時刻に達するまで待機し、それから JSON タイムラインの処理を開始します。

別ターミナルで、例えば 120 秒後に揃えたい場合:

  SYNC=$(date -d 'now + 120 seconds' +%s)
  echo $SYNC   # この値を控える

Publisher 側:

  export ROS_MASTER_URI=http://127.0.0.1:11311
  export ROS_IP=127.0.0.1

  VEHICLE_ID=vehicle-k \
  REGION_TIMELINE_DIR="/home/tavivid/v2x-pcd/k15-44-59" \
  REGION_TIMELINE_STEP_MS=90 \
  TIMELINE_SYNC_UNIX=$SYNC \
  ./gradlew --no-daemon :vehicle:runRos

Subscriber 側:

  export ROS_MASTER_URI=http://127.0.0.1:11311
  export ROS_IP=127.0.0.1

  VEHICLE_ID=vehicle-m \
  SUB_TIMELINE_DIR="/home/tavivid/v2x-pcd/subscription/m" \
  SUB_TIMELINE_STEP_MS=100 \
  TIMELINE_SYNC_UNIX=$SYNC \
  ./gradlew --no-daemon :vehicle:runRos

両者とも指定 Unix 時刻まで待機し、その後に各タイムラインスレッドの処理が始まります。


========================================
■ 8. よくあるつまずき・チェックリスト（rosjava 版）
========================================

- roscore を起動していない / ROS_MASTER_URI が不正
  - rosjava ノードは ROS master と疎通できないとトピックを publish/subscribe できません。
  - 別ターミナルで roscore を起動し、
    Vehicle 側で ROS_MASTER_URI / ROS_IP を正しく設定してください。

- REGION_TIMELINE_DIR / SUB_TIMELINE_DIR が間違っている
  - ディレクトリパスの Typo などで JSON が 1 つも見つからないと、
    タイムラインが開始されません。
  - `[PUB-TL] no timeline json under ...` / `[SUB-TL] no timeline json under ...`
    といったログがないか確認してください。

- 受信点群が保存されない
  - SUB_TIMELINE_* を設定しているか
  - `/home/tavivid/v2x-pcd/received` に書き込み権限があるか
  - Publisher 側で十分な点数（>10点）のチャンクが送られているか
    （極小チャンクはスキップされます）

- Publisher が早々に「no more data for region=...」
  - そのリージョンの PCD が読み尽くされているか、
    すべての PCD が 10点以下でスキップされている可能性があります。

- ログが多すぎて見づらい
  - rosjava の内部ログ(org.ros.internal.node.RosoutLogger など)が
    うるさい場合は、java.util.logging の設定や VehicleTimelineNode の中で
    Logger レベルを調整して抑制できます。
  - VehicleTimelineNode 独自のログは `[MM:SS.mmm] [TAG] ...` 形式で
    標準出力に出るようになっています。


========================================
■ 9. 設定の上書き（抜粋）
========================================

- v2x-config.yml (config/v2x-config.yml, vehicle/src/main/resources/v2x-config.yml)
  - vehicleId
  - datasetPath / datasetGlob / datasetLoop
  - 旧構成用のポート設定など

これらは環境変数や -Dsystem.property で上書き可能です（詳細は AppConfig を参照）。

rosjava / ROS1 タイムライン実験では主に以下を使います:

  - VEHICLE_ID
  - DATASET_PATH / DATASET_GLOB / DATASET_LOOP
  - REGION_TIMELINE_* / SUB_TIMELINE_*
  - TIMELINE_SYNC_UNIX
  - ROS_MASTER_URI / ROS_IP

以上
