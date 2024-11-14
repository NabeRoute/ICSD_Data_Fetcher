import os
import pandas as pd
from pymatgen.core import Composition
import asyncio
import aiohttp
import logging
from tqdm.asyncio import tqdm_asyncio
import aiofiles
from dotenv import load_dotenv
from aiohttp import ClientError, ClientResponseError
import argparse
from bs4 import BeautifulSoup
import io
import json

def safe_composition(f):
    """有効な化学式からreduced_formulaを取得。無効な場合はNoneを返す。"""
    try:
        return Composition(str(f)).reduced_formula
    except Exception as e:
        logging.warning(f"無効なStructuredFormula '{f}' をスキップしました。エラー: {e}")
        return None

class ICSDFetcher:
    def __init__(self, anion_list, num_elements_list, property_list, base_folder, downloaded_ids_file="downloaded_ids.txt"):
        """
        ICSDFetcherの初期化

        Parameters:
            anion_list (list): 検索対象のアニオンリスト
            num_elements_list (list): 検索対象の元素数リスト
            property_list (list): 取得する追加プロパティのリスト
            base_folder (str): CIFファイルと粉末パターン画像の保存フォルダのベースパス
            downloaded_ids_file (str): ダウンロード済みIDのログファイルパス
        """
        self.anion_list = anion_list
        self.num_elements_list = num_elements_list
        self.property_list = property_list
        self.downloaded_ids_file = downloaded_ids_file
        self.base_url = "https://icsd.fiz-karlsruhe.de/ws"
        self.semaphore = asyncio.Semaphore(30)  # 同時に30個のリクエストを許可
        self.lock = asyncio.Lock()  # ログファイル書き込み用のロック

        # フォルダ名の動的生成
        anion_part = "_".join(self.anion_list)
        num_elements_part = "_".join(map(str, self.num_elements_list))
        self.folder = os.path.join(base_folder, f"{anion_part}_elements_{num_elements_part}")

        # CIF、画像、XYエクスポートの保存フォルダを別々に設定
        self.cif_folder = os.path.join(self.folder, "cifs")
        self.image_folder = os.path.join(self.folder, "images")
        self.xy_export_folder = os.path.join(self.folder, "xy_exports")

        # 保存フォルダとサブフォルダが存在しない場合は作成
        for path in [self.folder, self.cif_folder, self.image_folder, self.xy_export_folder]:
            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                    logging.info(f"フォルダ '{path}' を作成しました。")
                except Exception as e:
                    logging.error(f"フォルダ '{path}' の作成に失敗しました。エラー: {e}")
                    raise

    async def load_downloaded_ids(self):
        """ダウンロード済みのIDをファイルから読み込み、セットとして返す"""
        if not os.path.exists(self.downloaded_ids_file):
            return set()
        async with aiofiles.open(self.downloaded_ids_file, 'r') as f:
            content = await f.read()
        downloaded_ids = set(content.strip().split('\n')) if content.strip() else set()
        logging.info(f"ダウンロード済みのID数: {len(downloaded_ids)}")
        return downloaded_ids

    async def log_downloaded_id(self, id_):
        """成功したIDをログファイルに非同期で書き込む"""
        async with self.lock:
            async with aiofiles.open(self.downloaded_ids_file, 'a') as f:
                await f.write(f"{id_}\n")

    async def fetch_with_retry(self, session, method, url, headers=None, params=None, data=None, retries=3, backoff=1):
        """リトライ機能を持つ非同期リクエスト関数"""
        for attempt in range(1, retries + 1):
            try:
                async with session.request(method, url, headers=headers, params=params, data=data) as response:
                    if response.status == 200:
                        return response
                    else:
                        error_content = await response.text()
                        logging.error(f"Attempt {attempt}: HTTP error {response.status} for {url}, 内容: {error_content}")
            except ClientError as e:
                logging.warning(f"Attempt {attempt}: Network error for {url}: {e}")
            
            if attempt < retries:
                logging.info(f"Retrying in {backoff} seconds...")
                await asyncio.sleep(backoff)
                backoff *= 2
        logging.error(f"All {retries} attempts failed for {url}")
        raise Exception(f"Failed to fetch {url} after {retries} attempts")

    async def authorize(self, session: aiohttp.ClientSession) -> tuple:
        """非同期でICSDにログインし、認証トークンと認証方法を取得する"""
        login_url = f"{self.base_url}/auth/login"
        data = {
            "loginid": os.getenv("ICSD_LOGIN_ID"),
            "password": os.getenv("ICSD_PASSWORD")
        }
        
        try:
            response = await self.fetch_with_retry(session, 'POST', login_url, data=data)
            token = response.headers.get("ICSD-Auth-Token")
            if token:
                logging.info("ログイン成功。認証トークンを取得しました。")
                auth_method = "token"
                logging.debug(f"認証後のauth_method: {auth_method}")  # デバッグログを追加
                return token, auth_method
            else:
                error_content = await response.text()
                logging.error(f"認証トークンが取得できませんでした。レスポンス内容: {error_content}")
                raise ValueError("認証トークンが取得できませんでした。")
        except Exception as e:
            logging.error(f"ログイン中にエラーが発生しました: {e}")
            raise

    async def fetch_data(self, session: aiohttp.ClientSession, token: str, anion: str, num_elements: int) -> pd.DataFrame:
        """非同期で指定されたアニオンと元素数のデータを取得する"""
        search_url = f"{self.base_url}/search/expert"
        query = f"composition : {anion} AND numberofelements : {num_elements}"
        params = {"query": query, "content type": "EXPERIMENTAL_INORGANIC"}
        headers = {"ICSD-Auth-Token": token}
    
        logging.info(f"Sending query: {query}")
    
        try:
            async with session.get(search_url, params=params, headers=headers) as response:
                content_type = response.headers.get('Content-Type', '')
                logging.info(f"Response Content-Type: {content_type}")
                if response.status == 200:
                    if 'application/xml' in content_type:
                        xml_content = await response.text()
                        soup = BeautifulSoup(xml_content, "lxml-xml")
                        idnums = soup.find_all('idnums')  # 'idnum'から'idnums'に変更
                        if idnums:
                            id_text = idnums[0].text.strip()
                            ids = id_text.split()  # スペースで分割
                            if ids:
                                # IDが数値であることを確認
                                valid_ids = [id_ for id_ in ids if id_.isdigit()]
                                if valid_ids:
                                    df = pd.DataFrame(valid_ids, columns=['id'])
                                    logging.info(f"{anion} のデータを取得しました。ID数: {len(valid_ids)}")
                                    return df
                                else:
                                    logging.error(f"{anion} のデータ取得中に有効なIDが見つかりませんでした。レスポンス内容（最初の500文字）:\n{xml_content[:500]}")
                                    return pd.DataFrame()
                            else:
                                logging.error(f"{anion} のデータ取得中にIDが見つかりませんでした。レスポンス内容（最初の500文字）:\n{xml_content[:500]}")
                                return pd.DataFrame()
                        else:
                            logging.error(f"{anion} のデータ取得中に'idnums'タグが見つかりませんでした。レスポンス内容（最初の500文字）:\n{xml_content[:500]}")
                            return pd.DataFrame()
                    else:
                        logging.error(f"{anion} のデータ取得中に期待されるContent-Typeが返されませんでした: {content_type}")
                        return pd.DataFrame()
                else:
                    error_content = await response.text()
                    logging.error(f"{anion} のデータ取得中にHTTPエラーが発生しました: {response.status}, 内容: {error_content}")
                    return pd.DataFrame()
        except ClientResponseError as e:
            logging.error(f"{anion} のデータ取得中にHTTPエラーが発生しました: {e.status}, メッセージ: {e.message}")
            return pd.DataFrame()
        except ClientError as e:
            logging.error(f"{anion} のデータ取得中にネットワークエラーが発生しました: {e}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"{anion} のデータ取得中に予期しないエラーが発生しました: {e}")
            return pd.DataFrame()

    async def writeout_cifs(self, session: aiohttp.ClientSession, token: str, ids: list):
        """非同期でCIFファイルを取得し保存する"""
        headers = {"ICSD-Auth-Token": token}
        
        tasks = [self.save_cif(session, headers, id_) for id_ in ids]
        await asyncio.gather(*tasks)

    async def save_cif(self, session: aiohttp.ClientSession, headers: dict, id_: str):
        """CIFファイルを取得して非同期で保存し、成功したIDをログに記録する"""
        url = f"{self.base_url}/cif/{id_}"
        async with self.semaphore:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        content = await response.text()
                        filename = os.path.join(self.cif_folder, f"{id_}.cif")
                        async with aiofiles.open(filename, "w") as f:
                            await f.write(content)
                        logging.info(f"{filename} を保存しました。")
                        await self.log_downloaded_id(id_)
                    else:
                        error_content = await response.text()
                        logging.error(f"ID {id_} のCIF取得中にHTTPエラーが発生しました: {response.status}, 内容: {error_content}")
                        if response.status == 403 and "Cif download limit exceeded" in error_content:
                            logging.error("CIFダウンロード制限に達しました。処理を中断します。")
                            exit(1)  # プログラムを終了
            except ClientResponseError as e:
                logging.error(f"ID {id_} のCIF取得中にHTTPエラーが発生しました: {e.status}, メッセージ: {e.message}")
            except ClientError as e:
                logging.error(f"ID {id_} のCIF取得中にネットワークエラーが発生しました: {e}")
            except Exception as e:
                logging.error(f"ID {id_} のCIF保存中にエラーが発生しました: {e}")
            finally:
                await asyncio.sleep(0.2)  # 各リクエストの間に0.2秒の遅延を挿入

    async def fetch_pp_image(self, session: aiohttp.ClientSession, token: str, idnum: int, width: int = 300, height: int = 300, radiation: str = "XRAY", dispersion: bool = False, lambda_val: float = 1.5418, xmin: float = 0.1, xmax: float = 60, xstep: float = 0.1, uparam: float = 0.05, vparam: float = -0.06, wparam: float = 0.07, plot: str = "THETA2", color: bool = False, intonly: bool = False, indices: bool = False, zzzparam: float = 0.55, standardized: bool = True):
        """非同期で粉末パターン画像を取得し保存する"""
        pp_url = f"{self.base_url}/pp/image/{idnum}"
        params = {
            "width": width,
            "height": height,
            "radiation": radiation,
            "dispersion": str(dispersion).lower(),
            "lambda": lambda_val,
            "xmin": xmin,
            "xmax": xmax,
            "xstep": xstep,
            "uparam": uparam,
            "vparam": vparam,
            "wparam": wparam,
            "plot": plot,
            "color": str(color).lower(),
            "intonly": str(intonly).lower(),
            "indices": str(indices).lower(),
            "zzzparam": zzzparam,
            "standardized": str(standardized).lower()
        }
        headers = {"ICSD-Auth-Token": token}
        filename = os.path.join(self.image_folder, f"{idnum}_powder_pattern.jpg")
        
        async with self.semaphore:
            try:
                async with session.get(pp_url, headers=headers, params=params) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        async with aiofiles.open(filename, "wb") as f:
                            await f.write(image_data)
                        logging.info(f"粉末パターン画像を保存しました: {filename}")
                    else:
                        error_content = await response.text()
                        logging.error(f"ID {idnum} の粉末パターン取得中にHTTPエラーが発生しました: {response.status}, 内容: {error_content}")
            except ClientResponseError as e:
                logging.error(f"ID {idnum} の粉末パターン取得中にHTTPエラーが発生しました: {e.status}, メッセージ: {e.message}")
            except ClientError as e:
                logging.error(f"ID {idnum} の粉末パターン取得中にネットワークエラーが発生しました: {e}")
            except Exception as e:
                logging.error(f"ID {idnum} の粉末パターン保存中にエラーが発生しました: {e}")

    async def fetch_xy_export(self, session: aiohttp.ClientSession, token: str, idnum: int, width: int, height: int, radiation: str, dispersion: bool, lambda_val: float, xmin: float, xmax: float, xzero: float, xstep: float, uparam: float, vparam: float, wparam: float, plot: str, color: bool, intonly: bool, indices: bool, zzzparam: float, standardized: bool):
        """非同期でgetXyExportを取得し保存する"""
        xy_export_url = f"{self.base_url}/pp/xyexport/{idnum}"  # 修正箇所
        params = {
            "width": width,
            "height": height,
            "radiation": radiation,
            "dispersion": str(dispersion).lower(),
            "lambda": lambda_val,
            "xmin": xmin,
            "xmax": xmax,
            "xzero": xzero,
            "xstep": xstep,
            "uparam": uparam,
            "vparam": vparam,
            "wparam": wparam,
            "plot": plot,
            "color": str(color).lower(),
            "intonly": str(intonly).lower(),
            "indices": str(indices).lower(),
            "zzzparam": zzzparam,
            "standardized": str(standardized).lower()
        }
        headers = {"ICSD-Auth-Token": token}
        filename = os.path.join(self.xy_export_folder, f"{idnum}_xy_export.csv")
        
        async with self.semaphore:
            try:
                async with session.get(xy_export_url, headers=headers, params=params) as response:
                    if response.status == 200:
                        export_data = await response.text()
                        async with aiofiles.open(filename, "w") as f:
                            await f.write(export_data)
                        logging.info(f"XYエクスポートデータを保存しました: {filename}")
                    else:
                        error_content = await response.text()
                        logging.error(f"ID {idnum} のXYエクスポート取得中にHTTPエラーが発生しました: {response.status}, 内容: {error_content}")
            except ClientResponseError as e:
                logging.error(f"ID {idnum} のXYエクスポート取得中にHTTPエラーが発生しました: {e.status}, メッセージ: {e.message}")
            except ClientError as e:
                logging.error(f"ID {idnum} のXYエクスポート取得中にネットワークエラーが発生しました: {e}")
            except Exception as e:
                logging.error(f"ID {idnum} のXYエクスポート保存中にエラーが発生しました: {e}")

    async def logout(self, session: aiohttp.ClientSession, token: str, auth_method: str):
        """非同期でICSDからログアウトする"""
        logging.debug(f"ログアウト時のauth_method: {auth_method}")  # デバッグログを追加
        if auth_method != "ip":
            logging.info("Token-based authenticationのため、ログアウト処理をスキップします。")
            return  # ログアウト処理をスキップ
        
        logout_url = f"{self.base_url}/auth/logout"
        headers = {"ICSD-Auth-Token": token}
        try:
            async with session.get(logout_url, headers=headers) as response:
                if response.status == 200:
                    logging.info("ログアウトに成功しました。")
                else:
                    error_content = await response.text()
                    logging.error(f"ログアウト中にHTTPエラーが発生しました: {response.status}, 内容: {error_content}")
        except ClientResponseError as e:
            logging.error(f"ログアウト中にHTTPエラーが発生しました: {e.status}, メッセージ: {e.message}")
        except ClientError as e:
            logging.error(f"ログアウト中にネットワークエラーが発生しました: {e}")
        except Exception as e:
            logging.error(f"ログアウト中に予期しないエラーが発生しました: {e}")

    async def fetch_properties(self, session: aiohttp.ClientSession, token: str, ids: list) -> pd.DataFrame:
        """指定されたIDの追加プロパティを取得する"""
        csv_url = f"{self.base_url}/csv"
        headers = {"ICSD-Auth-Token": token} if token else {}
    
        async def fetch_chunk(chunk_ids):
            async with self.semaphore:
                params = {
                    "idnum": chunk_ids,  # リストとして渡す
                    "listSelection": self.property_list,  # リストとして渡す
                    "windowsclient": "false"
                }
                # リクエスト内容をログに記録
                logging.debug(f"Fetching properties for IDs: {chunk_ids}")
                logging.debug(f"Request params: {params}")
                try:
                    async with session.get(csv_url, headers=headers, params=params) as response:
                        if response.status == 200:
                            csv_content = await response.text()
                            # pandasでCSVを読み込む
                            df = pd.read_csv(io.StringIO(csv_content), sep='\t')
                            logging.info(f"追加プロパティを取得しました。ID数: {len(chunk_ids)}")
                            logging.debug(f"取得したプロパティデータフレームのカラム: {df.columns.tolist()}")

                            # 不要なカラムを除去
                            df = df.drop(columns=['Unnamed: 2'], errors='ignore')

                            # IDの数とデータフレームの行数が一致するか確認
                            if len(df) != len(chunk_ids):
                                logging.error(f"ID数と取得したデータ行数が一致しません。ID数: {len(chunk_ids)}, 行数: {len(df)}")
                                return pd.DataFrame()
                            
                            # リクエスト時のIDを新たな 'id' カラムとして追加
                            df['id'] = chunk_ids
                            return df
                        else:
                            error_content = await response.text()
                            logging.error(f"追加プロパティ取得中にHTTPエラーが発生しました: {response.status}, 内容: {error_content}")
                            return pd.DataFrame()
                except ClientError as e:
                    logging.error(f"追加プロパティ取得中にネットワークエラーが発生しました: {e}")
                    return pd.DataFrame()
                except Exception as e:
                    logging.error(f"追加プロパティ取得中に予期しないエラーが発生しました: {e}")
                    return pd.DataFrame()
    
        # IDを500個ずつのチャンクに分割
        chunk_size = 500
        chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]
        tasks = [fetch_chunk(chunk) for chunk in chunks]
        results = await asyncio.gather(*tasks)
        # DataFrameを結合
        if results and not all(df.empty for df in results):
            properties_df = pd.concat(results, ignore_index=True)
        else:
            properties_df = pd.DataFrame()
        return properties_df

    async def run_normal_mode(self, session: aiohttp.ClientSession, token: str, downloaded_ids: set, download_ppimg: bool = False, pp_params: dict = None, get_xy_export: bool = False, xy_export_params: dict = None):
        """通常モードの処理"""
        # デフォルトパラメータの設定（空の場合）
        if pp_params is None:
            pp_params = {}
        if xy_export_params is None:
            xy_export_params = {}

        # データを非同期で取得
        tasks = [
            self.fetch_data(session, token, anion, num_elements) 
            for anion in self.anion_list 
            for num_elements in self.num_elements_list
        ]
        dfs = await tqdm_asyncio.gather(*tasks, desc="データ収集中")

        # DataFrameの結合と整形
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            logging.info(f"取得したデータフレームの形状: {df.shape}")
            logging.info(f"取得したデータフレームのカラム: {df.columns.tolist()}")
        else:
            df = pd.DataFrame()
            logging.warning("データが取得されませんでした。")
            return

        if not df.empty and 'id' in df.columns:
            # 未ダウンロードのIDのみを抽出
            all_ids = set(df['id'].tolist())
            ids_to_download = list(all_ids - downloaded_ids)
            logging.info(f"未ダウンロードのID数: {len(ids_to_download)}")

            if ids_to_download:
                # IDリストをバッチサイズごとに分割
                batch_size = 1000
                for i in range(0, len(ids_to_download), batch_size):
                    batch_ids = ids_to_download[i:i + batch_size]
                    logging.info(f"バッチ {i // batch_size + 1}: {len(batch_ids)} 件のCIFファイルをダウンロードします。")

                    # 新しいセッションを開始
                    async with aiohttp.ClientSession() as batch_session:
                        try:
                            # 新しいセッションでログイン
                            batch_token, batch_auth_method = await self.authorize(batch_session)

                            # ダウンロード
                            await self.writeout_cifs(batch_session, batch_token, batch_ids)

                            # ログアウト
                            await self.logout(batch_session, batch_token, batch_auth_method)
                        except Exception as e:
                            logging.error(f"バッチ {i // batch_size + 1} の処理中にエラーが発生しました: {e}")
                            continue

                # 追加プロパティの取得前に再認証を行う
                try:
                    logging.info("追加プロパティ取得前に再認証を行います。")
                    token, auth_method = await self.authorize(session)  # 再認証して新しいトークンと認証方法を取得
                except Exception as e:
                    logging.error(f"追加プロパティ取得前の再認証に失敗しました: {e}")
                    return

                # 追加プロパティの取得（全てのダウンロード済みIDに対して実行）
                properties_df = await self.fetch_properties(session, token, list(all_ids - downloaded_ids))
                if not properties_df.empty:
                    # 'id' カラムでマージ
                    df = df.merge(properties_df, on='id', how='left')
                    logging.info("追加プロパティをデータフレームにマージしました。")
                else:
                    logging.warning("追加プロパティが取得できませんでした。")
                    # dfに空のプロパティカラムを追加
                    for prop in self.property_list:
                        df[prop] = None

                # reduced_formula を計算
                if 'StructuredFormula' in df.columns:
                    df['reduced_formula'] = df['StructuredFormula'].apply(safe_composition)
                    df = df.dropna(subset=['reduced_formula'])
                else:
                    logging.warning("期待されるカラム 'StructuredFormula' が存在しません。")
                    df['reduced_formula'] = None

                # CSVとして保存
                elements_str = "_".join(map(str, self.num_elements_list))
                output_path = os.path.join(self.folder, f'icsd_dataset_{elements_str}_elements.csv')
                try:
                    df.to_csv(output_path, index=False)
                    logging.info(f"データが {output_path} に保存されました。")
                except Exception as e:
                    logging.error(f"CSV保存中にエラーが発生しました: {e}")
                
                # 粉末パターン画像のダウンロード
                if download_ppimg:
                    logging.info("粉末パターン画像のダウンロードを開始します。")
                    # デフォルトパラメータを設定（必要に応じてオーバーライド）
                    default_pp_params = {
                        "width": 300,
                        "height": 300,
                        "radiation": "XRAY",
                        "dispersion": False,
                        "lambda_val": 1.5418,
                        "xmin": 0.1,
                        "xmax": 60,
                        "xstep": 0.1,
                        "uparam": 0.05,
                        "vparam": -0.06,
                        "wparam": 0.07,
                        "plot": "THETA2",
                        "color": False,
                        "intonly": False,
                        "indices": False,
                        "zzzparam": 0.55,
                        "standardized": True
                    }
                    # ユーザーが提供したパラメータでデフォルトを更新
                    pp_params_combined = default_pp_params.copy()
                    pp_params_combined.update(pp_params)
                    
                    tasks_pp = [
                        self.fetch_pp_image(session, token, int(idnum), **pp_params_combined)
                        for idnum in ids_to_download
                    ]
                    await tqdm_asyncio.gather(*tasks_pp, desc="粉末パターン画像をダウンロード中")
                    logging.info("粉末パターン画像のダウンロードが完了しました。")
                
                # XYエクスポートの取得
                if get_xy_export:
                    logging.info("XYエクスポートの取得を開始します。")
                    # デフォルトパラメータを設定（必要に応じてオーバーライド）
                    default_xy_export_params = {
                        "width": 300,
                        "height": 300,
                        "radiation": "XRAY",
                        "dispersion": False,
                        "lambda_val": 1.5418,
                        "xmin": 0.1,
                        "xmax": 60,
                        "xzero": 0.0,
                        "xstep": 0.1,
                        "uparam": 0.05,
                        "vparam": -0.06,
                        "wparam": 0.07,
                        "plot": "THETA2",
                        "color": False,
                        "intonly": False,
                        "indices": False,
                        "zzzparam": 0.55,
                        "standardized": True
                    }
                    # ユーザーが提供したパラメータでデフォルトを更新
                    xy_export_params_combined = default_xy_export_params.copy()
                    xy_export_params_combined.update(xy_export_params)
                    
                    tasks_xy = [
                        self.fetch_xy_export(
                            session, token, int(idnum),
                            width=xy_export_params_combined.get("width", 300),
                            height=xy_export_params_combined.get("height", 300),
                            radiation=xy_export_params_combined.get("radiation", "XRAY"),
                            dispersion=xy_export_params_combined.get("dispersion", False),
                            lambda_val=xy_export_params_combined.get("lambda_val", 1.5418),
                            xmin=xy_export_params_combined.get("xmin", 0.1),
                            xmax=xy_export_params_combined.get("xmax", 60),
                            xzero=xy_export_params_combined.get("xzero", 0.0),
                            xstep=xy_export_params_combined.get("xstep", 0.1),
                            uparam=xy_export_params_combined.get("uparam", 0.05),
                            vparam=xy_export_params_combined.get("vparam", -0.06),
                            wparam=xy_export_params_combined.get("wparam", 0.07),
                            plot=xy_export_params_combined.get("plot", "THETA2"),
                            color=xy_export_params_combined.get("color", False),
                            intonly=xy_export_params_combined.get("intonly", False),
                            indices=xy_export_params_combined.get("indices", False),
                            zzzparam=xy_export_params_combined.get("zzzparam", 0.55),
                            standardized=xy_export_params_combined.get("standardized", True)
                        )
                        for idnum in ids_to_download
                    ]
                    await tqdm_asyncio.gather(*tasks_xy, desc="XYエクスポートを取得中")
                    logging.info("XYエクスポートの取得が完了しました。")
        else:
            logging.info("全てのIDが既にダウンロード済みです。")


    async def run_test_mode(self, session: aiohttp.ClientSession, token: str, test_ids: list, download_ppimg: bool = False, pp_params: dict = None, get_xy_export: bool = False, xy_export_params: dict = None):
        """テストモードの処理"""
        if not test_ids:
            logging.error("テスト用のIDが指定されていません。")
            return

        try:
            # 追加プロパティの取得
            properties_df = await self.fetch_properties(session, token, test_ids)
            if not properties_df.empty:
                # 'id' カラムでマージ
                df_test = pd.DataFrame(test_ids, columns=['id'])
                df_test = df_test.merge(properties_df, on='id', how='left')
                logging.info("追加プロパティをデータフレームにマージしました。")
            else:
                logging.warning("追加プロパティが取得できませんでした。")
                # df_testを初期化し、プロパティカラムをNoneで埋める
                df_test = pd.DataFrame(test_ids, columns=['id'])
                for prop in self.property_list:
                    df_test[prop] = None

            # reduced_formula を計算
            if 'StructuredFormula' in properties_df.columns and not properties_df.empty:
                df_test['reduced_formula'] = df_test['StructuredFormula'].apply(safe_composition)
                df_test = df_test.dropna(subset=['reduced_formula'])
            else:
                logging.warning("期待されるカラム 'StructuredFormula' が存在しません。")
                df_test['reduced_formula'] = None

            # CSVとして保存
            elements_str = "_".join(map(str, self.num_elements_list))
            output_test_path = os.path.join(self.folder, f'icsd_dataset_test_mode.csv')
            try:
                df_test.to_csv(output_test_path, index=False)
                logging.info(f"テストデータが {output_test_path} に保存されました。")
            except Exception as e:
                logging.error(f"CSV保存中にエラーが発生しました: {e}")

            # 粉末パターン画像のダウンロード（テスト用）
            if download_ppimg:
                logging.info("テスト用の粉末パターン画像をダウンロードします。")
                # デフォルトパラメータを設定（必要に応じてオーバーライド）
                default_pp_params = {
                    "width": 300,
                    "height": 300,
                    "radiation": "XRAY",
                    "dispersion": False,
                    "lambda_val": 1.5418,
                    "xmin": 0.1,
                    "xmax": 60,
                    "xstep": 0.1,
                    "uparam": 0.05,
                    "vparam": -0.06,
                    "wparam": 0.07,
                    "plot": "THETA2",
                    "color": False,
                    "intonly": False,
                    "indices": False,
                    "zzzparam": 0.55,
                    "standardized": True
                }
                # ユーザーが提供したパラメータでデフォルトを更新
                pp_params_combined = default_pp_params.copy()
                if pp_params:
                    pp_params_combined.update(pp_params)
                
                tasks_pp = [
                    self.fetch_pp_image(session, token, int(idnum), **pp_params_combined)
                    for idnum in test_ids
                ]
                await tqdm_asyncio.gather(*tasks_pp, desc="テスト用粉末パターン画像をダウンロード中")
                logging.info("テスト用粉末パターン画像のダウンロードが完了しました。")
            
            # XYエクスポートの取得（テスト用）
            if get_xy_export:
                logging.info("テスト用のXYエクスポートを取得します。")
                # デフォルトパラメータを設定（必要に応じてオーバーライド）
                default_xy_export_params = {
                    "width": 300,
                    "height": 300,
                    "radiation": "XRAY",
                    "dispersion": False,
                    "lambda_val": 1.5418,
                    "xmin": 0.1,
                    "xmax": 60,
                    "xzero": 0.0,
                    "xstep": 0.1,
                    "uparam": 0.05,
                    "vparam": -0.06,
                    "wparam": 0.07,
                    "plot": "THETA2",
                    "color": False,
                    "intonly": False,
                    "indices": False,
                    "zzzparam": 0.55,
                    "standardized": True
                }
                # ユーザーが提供したパラメータでデフォルトを更新
                xy_export_params_combined = default_xy_export_params.copy()
                if xy_export_params:
                    xy_export_params_combined.update(xy_export_params)
                
                tasks_xy = [
                    self.fetch_xy_export(
                        session, token, int(idnum),
                        width=xy_export_params_combined.get("width", 300),
                        height=xy_export_params_combined.get("height", 300),
                        radiation=xy_export_params_combined.get("radiation", "XRAY"),
                        dispersion=xy_export_params_combined.get("dispersion", False),
                        lambda_val=xy_export_params_combined.get("lambda_val", 1.5418),
                        xmin=xy_export_params_combined.get("xmin", 0.1),
                        xmax=xy_export_params_combined.get("xmax", 60),
                        xzero=xy_export_params_combined.get("xzero", 0.0),
                        xstep=xy_export_params_combined.get("xstep", 0.1),
                        uparam=xy_export_params_combined.get("uparam", 0.05),
                        vparam=xy_export_params_combined.get("vparam", -0.06),
                        wparam=xy_export_params_combined.get("wparam", 0.07),
                        plot=xy_export_params_combined.get("plot", "THETA2"),
                        color=xy_export_params_combined.get("color", False),
                        intonly=xy_export_params_combined.get("intonly", False),
                        indices=xy_export_params_combined.get("indices", False),
                        zzzparam=xy_export_params_combined.get("zzzparam", 0.55),
                        standardized=xy_export_params_combined.get("standardized", True)
                    )
                    for idnum in test_ids
                ]
                await tqdm_asyncio.gather(*tasks_xy, desc="テスト用XYエクスポートを取得中")
                logging.info("テスト用XYエクスポートの取得が完了しました。")

        except Exception as e:
            logging.error(f"テストモード中にエラーが発生しました: {e}")

    async def run(self, test_mode=False, test_ids=None, download_ppimg=False, pp_params=None, get_xy_export=False, xy_export_params=None):
        """
        スクリプトのメイン実行関数

        Parameters:
            test_mode (bool): テストモードを有効にするかどうか
            test_ids (list): テスト用のIDリスト
            download_ppimg (bool): 粉末パターン画像をダウンロードするかどうか
            pp_params (dict): 粉末パターン画像の取得パラメータ
            get_xy_export (bool): XYエクスポートを取得するかどうか
            xy_export_params (dict): XYエクスポートの取得パラメータ
        """
        downloaded_ids = await self.load_downloaded_ids()

        async with aiohttp.ClientSession() as session:
            try:
                if not test_mode:
                    # 通常モードの処理
                    # ログインして認証トークンと認証方法を取得
                    token, auth_method = await self.authorize(session)

                    # 通常モードの処理を実行
                    await self.run_normal_mode(session, token, downloaded_ids, download_ppimg, pp_params, get_xy_export, xy_export_params)

                    # ログアウト処理
                    await self.logout(session, token, auth_method)
                else:
                    # テストモードの処理
                    if not test_ids:
                        # ダウンロード済みのIDから少数選択（例：5件）
                        test_ids = list(downloaded_ids)[:5]
                        logging.info(f"テスト用のIDを選択しました: {test_ids}")
                    
                    # テストモードでも認証トークンと認証方法を取得
                    token, auth_method = await self.authorize(session)

                    await self.run_test_mode(session, token, test_ids, download_ppimg, pp_params, get_xy_export, xy_export_params)

                    # ログアウト処理
                    await self.logout(session, token, auth_method)

            except Exception as e:
                logging.error(f"メイン処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    # 環境変数のロード
    load_dotenv()

    # ログの設定
    logging.basicConfig(
        level=logging.DEBUG,  # デバッグログを有効にする
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("fetch_data_async.log"),
            logging.StreamHandler()
        ]
    )

    def parse_args():
        parser = argparse.ArgumentParser(description="ICSDデータを非同期で取得するスクリプト")
        parser.add_argument("--anion", type=str, nargs="+", required=True, help="検索対象のアニオンリスト")
        parser.add_argument("--num_elements", type=int, nargs="+", required=True, help="検索対象の元素数リスト")
        parser.add_argument("--base_folder", type=str, default="./icsd_saved", help="CIFファイルと粉末パターン画像の保存フォルダのベースパス")
        parser.add_argument("--test_properties", action='store_true', help="CIFダウンロードをスキップして追加プロパティの取得をテストする")
        parser.add_argument("--test_ids", type=str, nargs="*", help="テスト用のIDリスト（スペース区切り）")
        parser.add_argument("--download_ppimg", action='store_true', help="粉末パターン画像をダウンロードする")
        parser.add_argument("--pp_params", type=str, default='{}', help="粉末パターン画像取得用のパラメータをJSON形式で指定する")
        parser.add_argument("--get_xy_export", action='store_true', help="XYエクスポートを取得する")
        parser.add_argument("--xy_export_params", type=str, default='{}', help="XYエクスポート取得用のパラメータをJSON形式で指定する")
        return parser.parse_args()

    async def main():
        # 引数の解析
        args = parse_args()
        
        # 取得するプロパティのリスト
        property_list = ["CollectionCode", "StructuredFormula"]
        
        # 粉末パターン画像のパラメータを解析
        try:
            pp_params = json.loads(args.pp_params)
        except json.JSONDecodeError as e:
            logging.error(f"粉末パターン画像パラメータの解析に失敗しました: {e}")
            pp_params = {}
        
        # XYエクスポートのパラメータを解析
        try:
            xy_export_params = json.loads(args.xy_export_params)
        except json.JSONDecodeError as e:
            logging.error(f"XYエクスポートパラメータの解析に失敗しました: {e}")
            xy_export_params = {}
        
        # ICSDFetcherクラスのインスタンスを作成
        fetcher = ICSDFetcher(
            anion_list=args.anion,
            num_elements_list=args.num_elements,
            property_list=property_list,
            base_folder=args.base_folder
        )
        
        # モードに応じて処理を実行
        await fetcher.run(
            test_mode=args.test_properties,
            test_ids=args.test_ids,
            download_ppimg=args.download_ppimg,
            pp_params=pp_params,
            get_xy_export=args.get_xy_export,
            xy_export_params=xy_export_params
        )

    asyncio.run(main())