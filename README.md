# ICSD Fetcher

ICSD Fetcherは、無機結晶構造データベース（ICSD）から結晶学データを効率的にダウンロードするための非同期Pythonツールです。CIFファイル、粉末回折パターン画像、XYエクスポートデータの一括ダウンロードをサポートしています。

## 主な機能

- 非同期処理による高速なデータ取得
- CIFファイルの一括ダウンロード
- 粉末回折パターン画像の生成とダウンロード
- XYエクスポートデータの取得
- アニオンと元素数によるフィルタリング
- 追加プロパティの取得
- 進捗状況の追跡とログ記録
- 中断されたダウンロードの再開機能
- 動作確認用のテストモード

## 必要条件

- Python 3.7以上
- 有効なICSDサブスクリプションとアカウント情報
- .envファイルを作成する必要があります。

## インストール手順

1. リポジトリのクローン：
```bash
git clone https://github.com/yourusername/icsd-fetcher.git
cd icsd-fetcher
```

2. 必要なパッケージのインストール：
```bash
pip install -r requirements.txt
```

3. プロジェクトのルートディレクトリに`.env`ファイルを作成し、ICSDの認証情報を記入：
```
ICSD_LOGIN_ID="Your ICSD Login ID"
ICSD_PASSWORD="Your ICSD Password"
```

## 使用方法

### 基本的な使用方法

スクリプトは以下のようにコマンドラインから実行できます：

```bash
python fetch_data.py --anion "O-2" "S-2" --num_elements 3 4 --base_folder "./icsd_data"
```

### 必須引数

- `--anion`: 検索対象のアニオンのリスト（例：`"O-2" "S-2"`）
- `--num_elements`: 検索対象の元素数のリスト（例：`3 4`）

### オプション引数

- `--base_folder`: ファイルの保存先ベースディレクトリ（デフォルト：`"./icsd_saved"`）
- `--test_properties`: プロパティ取得を検証するテストモードの有効化
- `--test_ids`: テスト用の特定のICSD ID指定
- `--download_ppimg`: 粉末回折パターン画像のダウンロード有効化
- `--pp_params`: 粉末回折パターンのパラメータ（JSON形式）
- `--get_xy_export`: XYエクスポートデータの取得有効化
- `--xy_export_params`: XYエクスポートのパラメータ（JSON形式）

### 使用例

1. 3元素または4元素の酸化物の検索：
```bash
python icsd_fetcher.py --anion "O-2" --num_elements 3 4
```

2. 硫化物の粉末回折パターン付きダウンロード：
```bash
python icsd_fetcher.py --anion "S-2" --num_elements 2 --download_ppimg
```

3. 特定のIDでのテストモード：
```bash
python icsd_fetcher.py --anion "O-2" --num_elements 3 --test_properties --test_ids "12345" "67890"
```

4. カスタム粉末回折パターンパラメータの指定：
```bash
python icsd_fetcher.py --anion "O-2" --num_elements 3 --download_ppimg --pp_params '{"width": 500, "height": 500, "xmax": 90}'
```


## ログ機能

スクリプトは詳細なログファイル（`fetch_data_async.log`）を作成し、以下の情報を記録します：
- 進捗状況
- エラーメッセージ
- ダウンロード状態
- APIレスポンスの詳細



## 謝辞

このツールはICSD Web Service APIを使用しています。使用にはICSDの有効なサブスクリプションが必要です。

## 免責事項

このツールはFIZ KarlsruheやICSDデータベースの公式ツールではありません。ユーザーは自身のICSDライセンス契約に従って使用する責任があります。


## パフォーマンスの最適化

- 同時ダウンロード数は30に設定されていますが、ネットワーク環境に応じて調整可能です
- 大規模なダウンロードの場合は、複数回に分けて実行することを推奨
- テストモードを使用して、本番実行前に設定の確認を推奨

