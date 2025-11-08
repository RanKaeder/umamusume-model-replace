import UnityPy
import sqlite3
import os
import shutil
import typing as t
import subprocess
from pathlib import Path
from PIL import Image
from . import assets_path

spath = os.path.split(__file__)[0]
BACKUP_PATH = f"{spath}/backup"
EDITED_PATH = f"{spath}/edited"
DECRYPTED_DAT_PATH = f"{spath}/dat_decrypted"
ENCRYPTED_DAT_PATH = f"{spath}/dat_encrypted"
DECRYPTED_DB_PATH = f"{spath}/meta_decrypted"


# UmaDecryptor.exe 的路径 - 从 umaModelReplace 文件夹获取
def get_decryptor_path():
    """
    获取 UmaDecryptor.exe 的完整路径
    优先级: 当前目录 (umaModelReplace/) > 项目根目录 > PATH中的exe
    """
    # 方法1: 当前目录（推荐，防止用户误操作）
    current_dir = Path(__file__).parent
    decryptor = current_dir / "UmaDecryptor.exe"
    if decryptor.exists():
        return str(decryptor.absolute())

    # 方法2: 项目根目录
    root_dir = Path(__file__).parent.parent
    decryptor = root_dir / "UmaDecryptor.exe"
    if decryptor.exists():
        return str(decryptor.absolute())

    # 方法3: 假设在 PATH 中
    return "UmaDecryptor.exe"


DECRYPTOR_PATH = get_decryptor_path()


class UmaFileNotFoundError(FileNotFoundError):
    pass


def replace_raw(data: bytes, old: bytes, new: bytes, context: int = 20) -> bytes:
    """
    在 data 中将所有 old 替换为 new，并在每次替换时打印上下文。

    :param data: 原始字节串
    :param old: 需要被替换的字节串
    :param new: 替换成的字节串
    :param context: 打印时，替换位置前后各保留多少字节上下文
    :return: 完成所有替换后的新字节串
    """

    any_replaced = False
    result = bytearray()
    i = 0
    while True:
        idx = data.find(old, i)
        if idx < 0:
            # 无更多匹配，添加剩余部分
            result.extend(data[i:])
            break
        any_replaced = True
        # 计算上下文的起止位置
        start = max(idx - context, 0)
        end = min(idx + len(old) + context, len(data))

        before = data[start:idx]
        match = data[idx:idx + len(old)]
        after = data[idx + len(old):end]

        # 打印信息
        # print(f"Match at byte offset {idx}:")
        # print(f"  …{before!r}[{match!r}]{after!r}…")

        # 构造结果
        result.extend(data[i:idx])
        result.extend(new)
        i = idx + len(old)

    # result=result.replace("chr1024_00/textures/tex_chr1024_00_cheek0".encode("utf8"), "chr9002_00/textures/tex_chr9002_00_cheek0".encode("utf8"))
    return bytes(result), any_replaced


class UmaReplace:
    def __init__(self):
        self.init_folders()
        profile_path = os.environ.get("UserProfile")
        self.base_path = f"{profile_path}/AppData/LocalLow/Cygames/umamusume"

        # 先解密 meta 数据库，再连接解密后的数据库
        self._decrypt_meta_db()
        self.conn = sqlite3.connect(f"{DECRYPTED_DB_PATH}/meta")
        self.master_conn = sqlite3.connect(f"{self.base_path}/master/master.mdb")

    def _run_decryptor(self, args: list) -> bool:
        """
        运行 UmaDecryptor.exe (静默处理)
        :param args: 命令行参数列表
        :return: 是否执行成功
        """
        try:
            cmd = [DECRYPTOR_PATH] + args
            # 静默运行，不打印输出
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300,
                encoding='utf-8',
                errors='ignore'
            )
            if result.returncode != 0:
                print(f"❌ 处理失败: {result.stderr if result.stderr else '未知错误'}")
                return False
            # 只在成功时给出简短提示
            return True
        except subprocess.TimeoutExpired:
            print(f"❌ 处理超时（超过5分钟）")
            return False
        except Exception as e:
            print(f"❌ 执行错误: {e}")
            return False

    def _decrypt_meta_db(self):
        """
        解密 meta 数据库到 edited 目录
        """
        meta_path = f"{self.base_path}/meta"
        if not os.path.isfile(meta_path):
            raise UmaFileNotFoundError(f"meta database not found at {meta_path}")

        # 清理旧的解密文件
        if os.path.isdir(DECRYPTED_DB_PATH):
            shutil.rmtree(DECRYPTED_DB_PATH)
        os.makedirs(DECRYPTED_DB_PATH, exist_ok=True)

        print(f"Decrypting meta database...")
        success = self._run_decryptor(["decrypt-db", "-i", meta_path, "-o", f"{DECRYPTED_DB_PATH}/meta"])
        if not success:
            raise RuntimeError("Failed to decrypt meta database")
        print("Meta database decrypted successfully")

    def _decrypt_dat_bundle(self, bundle_hash: str) -> str:
        """
        解密单个 dat 文件到临时目录
        :param bundle_hash: bundle 的 hash
        :return: 解密后的文件路径
        """
        original_path = self.get_bundle_path(bundle_hash)
        if not os.path.isfile(original_path):
            raise UmaFileNotFoundError(f"Bundle file not found: {original_path}")

        # 创建临时目录结构
        temp_encrypted_dir = f"{ENCRYPTED_DAT_PATH}/dat/{bundle_hash[:2]}"
        os.makedirs(temp_encrypted_dir, exist_ok=True)

        temp_file = f"{temp_encrypted_dir}/{bundle_hash}"

        # 复制原始加密文件到临时目录
        shutil.copyfile(original_path, temp_file)

        decrypted_path = f"{DECRYPTED_DAT_PATH}/dat/{bundle_hash[:2]}/{bundle_hash}"
        return decrypted_path

    def _decrypt_dat_bundles_batch(self, bundle_hashes: list) -> list:
        """
        批量解密多个 dat 文件
        :param bundle_hashes: bundle hash 列表
        :return: 解密后的文件路径列表
        """
        if not bundle_hashes:
            return []

        # 清理旧的解密目录 - 重试机制处理文件被占用的情况
        if os.path.isdir(DECRYPTED_DAT_PATH):
            try:
                shutil.rmtree(DECRYPTED_DAT_PATH)
            except PermissionError:
                # 文件被占用，等待后重试
                import time
                print("⚠️  Waiting for files to be released...")
                time.sleep(1)
                try:
                    shutil.rmtree(DECRYPTED_DAT_PATH)
                except PermissionError:
                    print("⚠️  Force removing locked directory...")
                    import time
                    time.sleep(2)
                    try:
                        shutil.rmtree(DECRYPTED_DAT_PATH)
                    except:
                        # 如果still无法删除，忽略错误继续
                        pass

        os.makedirs(DECRYPTED_DAT_PATH, exist_ok=True)

        # 清理旧的临时加密目录
        if os.path.isdir(ENCRYPTED_DAT_PATH):
            try:
                shutil.rmtree(ENCRYPTED_DAT_PATH)
            except PermissionError:
                import time
                time.sleep(1)
                try:
                    shutil.rmtree(ENCRYPTED_DAT_PATH)
                except:
                    pass

        # 将所有需要解密的文件复制到临时目录
        print(f"Copying {len(bundle_hashes)} files to temporary directory...")
        for bundle_hash in bundle_hashes:
            original_path = self.get_bundle_path(bundle_hash)
            if not os.path.isfile(original_path):
                print(f"⚠️  File not found: {original_path}")
                continue

            temp_encrypted_dir = f"{ENCRYPTED_DAT_PATH}/dat/{bundle_hash[:2]}"
            os.makedirs(temp_encrypted_dir, exist_ok=True)
            temp_file = f"{temp_encrypted_dir}/{bundle_hash}"
            shutil.copyfile(original_path, temp_file)

        # 一次性执行解密
        print(f"Decrypting {len(bundle_hashes)} bundles...")
        success = self._run_decryptor([
            "decrypt-dat",
            "-i", ENCRYPTED_DAT_PATH,
            "-o", DECRYPTED_DAT_PATH,
            "-m", f"{DECRYPTED_DB_PATH}/meta"
        ])

        if not success:
            raise RuntimeError(f"Failed to decrypt bundles")

        # 验证并返回解密后的文件路径
        decrypted_paths = []
        for bundle_hash in bundle_hashes:
            decrypted_path = f"{DECRYPTED_DAT_PATH}/dat/{bundle_hash[:2]}/{bundle_hash}"
            if os.path.isfile(decrypted_path):
                decrypted_paths.append(decrypted_path)
            else:
                print(f"⚠️  Decrypted file not found: {decrypted_path}")

        print(f"Successfully decrypted {len(decrypted_paths)} files")
        return decrypted_paths

    def _encrypt_dat_bundle(self, decrypted_file_path: str, bundle_hash: str) -> str:
        """
        加密已解密的 dat 文件回到原始位置
        :param decrypted_file_path: 解密后的文件路径
        :param bundle_hash: bundle 的 hash
        :return: 加密后的文件路径
        """
        if not os.path.isfile(decrypted_file_path):
            raise UmaFileNotFoundError(f"Decrypted file not found: {decrypted_file_path}")

        # 由于异或处理，加密和解密使用相同的命令
        # 创建临时目录放置要加密的文件
        temp_decrypt_dir = f"{DECRYPTED_DAT_PATH}_temp"
        temp_decrypt_dat_dir = f"{temp_decrypt_dir}/dat/{bundle_hash[:2]}"
        os.makedirs(temp_decrypt_dat_dir, exist_ok=True)

        # 复制解密后的文件到临时目录
        temp_decrypt_file = f"{temp_decrypt_dat_dir}/{bundle_hash}"
        shutil.copyfile(decrypted_file_path, temp_decrypt_file)

        # 创建输出目录
        temp_encrypted_output = f"{ENCRYPTED_DAT_PATH}_output"
        os.makedirs(temp_encrypted_output, exist_ok=True)

        # 加密文件（使用相同的 decrypt 命令，因为是异或操作）
        print(f"Encrypting bundle: {bundle_hash}")
        success = self._run_decryptor([
            "decrypt-dat",
            "-i", temp_decrypt_dir,
            "-o", temp_encrypted_output,
            "-m", f"{DECRYPTED_DB_PATH}/meta"
        ])

        if not success:
            raise RuntimeError(f"Failed to encrypt bundle {bundle_hash}")

        encrypted_path = f"{temp_encrypted_output}/dat/{bundle_hash[:2]}/{bundle_hash}"
        if not os.path.isfile(encrypted_path):
            raise RuntimeError(f"Encrypted file not found: {encrypted_path}")

        print(f"Bundle encrypted: {encrypted_path}")
        return encrypted_path

    def _encrypt_dat_bundles_batch(self, decrypted_file_paths: list, bundle_hashes: list) -> list:
        """
        批量加密多个 dat 文件
        :param decrypted_file_paths: 解密后的文件路径列表
        :param bundle_hashes: 对应的 bundle hash 列表
        :return: 加密后的文件路径列表
        """
        if not decrypted_file_paths:
            return []

        # 清理旧的临时目录
        temp_decrypt_dir = f"{DECRYPTED_DAT_PATH}_temp"
        if os.path.isdir(temp_decrypt_dir):
            shutil.rmtree(temp_decrypt_dir)

        temp_encrypted_output = f"{ENCRYPTED_DAT_PATH}_output"
        if os.path.isdir(temp_encrypted_output):
            shutil.rmtree(temp_encrypted_output)

        # 将所有解密后的文件（包括修改过的）复制到临时目录
        print(f"Copying {len(decrypted_file_paths)} files for encryption...")
        for i, decrypted_path in enumerate(decrypted_file_paths):
            if not os.path.isfile(decrypted_path):
                print(f"⚠️  File not found: {decrypted_path}")
                continue

            bundle_hash = bundle_hashes[i]
            temp_decrypt_dat_dir = f"{temp_decrypt_dir}/dat/{bundle_hash[:2]}"
            os.makedirs(temp_decrypt_dat_dir, exist_ok=True)
            temp_decrypt_file = f"{temp_decrypt_dat_dir}/{bundle_hash}"
            shutil.copyfile(decrypted_path, temp_decrypt_file)

        # 一次性执行加密（异或处理）
        print(f"Encrypting {len(decrypted_file_paths)} bundles...")
        success = self._run_decryptor([
            "decrypt-dat",
            "-i", temp_decrypt_dir,
            "-o", temp_encrypted_output,
            "-m", f"{DECRYPTED_DB_PATH}/meta"
        ])

        if not success:
            raise RuntimeError(f"Failed to encrypt bundles")

        # 验证并返回加密后的文件路径
        encrypted_paths = []
        for bundle_hash in bundle_hashes:
            encrypted_path = f"{temp_encrypted_output}/dat/{bundle_hash[:2]}/{bundle_hash}"
            if os.path.isfile(encrypted_path):
                encrypted_paths.append(encrypted_path)
            else:
                print(f"⚠️  Encrypted file not found: {encrypted_path}")

        print(f"Successfully encrypted {len(encrypted_paths)} files")
        return encrypted_paths

    def _cleanup_temp_dirs(self):
        """
        清理临时目录
        """
        for temp_dir in [ENCRYPTED_DAT_PATH, f"{DECRYPTED_DAT_PATH}_temp", f"{ENCRYPTED_DAT_PATH}_output"]:
            if os.path.isdir(temp_dir):
                shutil.rmtree(temp_dir)
                print(f"Cleaned up: {temp_dir}")

    @staticmethod
    def init_folders():
        if not os.path.isdir(BACKUP_PATH):
            os.makedirs(BACKUP_PATH)
        if not os.path.isdir(EDITED_PATH):
            os.makedirs(EDITED_PATH)

    def get_bundle_path(self, bundle_hash: str):
        return f"{self.base_path}/dat/{bundle_hash[:2]}/{bundle_hash}"

    def get_bundle_hash(self, path: str, query_orig_id: t.Optional[str]) -> str:
        """
        通过资源路径查询对应的 bundle hash
        :param path: 资源路径，例如: "3d/chara/body/bdy1046/pfb_bdy1046"
        :param query_orig_id: 原始 ID，用于模糊查询
        :return: bundle hash
        """
        cursor = self.conn.cursor()
        query = cursor.execute("SELECT h FROM a WHERE n=?", [path]).fetchone()
        if query is None:
            if (query_orig_id is not None) and ("_" in query_orig_id):
                query_id, query_sub_id = query_orig_id.split("_")

                if query is None:
                    new_path = path.replace(query_orig_id, f"{query_id}_%")
                    query = cursor.execute("SELECT h, n FROM a WHERE n LIKE ?", [new_path]).fetchone()
                    if query is not None:
                        print(f"{path} not found, but found {query[1]}")

        if query is None:
            raise UmaFileNotFoundError(f"{path} not found!")

        cursor.close()
        return query[0]

    def file_backup(self, bundle_hash: str):
        if not os.path.isfile(f"{BACKUP_PATH}/{bundle_hash}"):
            shutil.copyfile(f"{self.get_bundle_path(bundle_hash)}", f"{BACKUP_PATH}/{bundle_hash}")

    def file_restore(self, hashs: t.Optional[t.List[str]] = None):
        """
        恢复备份
        :param hashs: bundle hash 列表, 若为 None, 则恢复备份文件夹内所有文件
        """
        if not hashs:
            hashs = os.listdir(BACKUP_PATH)
        if not isinstance(hashs, list):
            raise TypeError(f"hashs must be a list, not {type(hashs)}")

        for i in hashs:
            fpath = f"{BACKUP_PATH}/{i}"
            if os.path.isfile(fpath):
                shutil.copyfile(fpath, self.get_bundle_path(i))
                print(f"restore {i}")

    @staticmethod
    def replace_file_path(fname: str, id1: str, id2: str, save_name: t.Optional[str] = None) -> str:
        env = UnityPy.load(fname)

        data = None

        for obj in env.objects:
            # if obj.type.name not in ["Avatar"]:
            data = obj.read()
            # print(obj.type.name, data.name)
            if obj.type.name == "MonoBehaviour":
                if (hasattr(data, "raw_data")):
                    raw = bytes(data.raw_data)
                    raw, changed = replace_raw(raw, old=id1.encode("utf8"), new=id2.encode("utf8"))
                    data.set_raw_data(raw)
                    data.save(raw_data=raw)
                    # if(changed):
                    #    print(data.m_Name)
                else:
                    raw = bytes(obj.get_raw_data())
                    raw, changed = replace_raw(raw, old=id1.encode("utf8"), new=id2.encode("utf8"))
                    obj.set_raw_data(raw)
                    # if(changed):
                    #    print(data.m_Name)

            else:
                # print(obj.type.name)
                raw = bytes(obj.get_raw_data())
                raw, changed = replace_raw(raw, old=id1.encode("utf8"), new=id2.encode("utf8"))
                obj.set_raw_data(raw)
                # if(changed):
                #    print(data.m_Name)
                # obj.save()

        if save_name is None:
            save_name = f"{EDITED_PATH}/{os.path.split(fname)[-1]}"
        if data is None:
            with open(fname, "rb") as f:
                data = f.read()
                data = data.replace(id1.encode("utf8"), id2.encode("utf8"))
            with open(save_name, "wb") as f:
                f.write(data)
        else:
            with open(save_name, "wb") as f:
                f.write(env.file.save())
        return save_name

    def replace_file_ids_with_encryption(self, orig_path: str, new_path: str, id_orig: str, id_new: str):
        """
        替换文件 ID，包含加密/解密处理
        :param orig_path: 原始资源路径
        :param new_path: 新资源路径
        :param id_orig: 原始 ID
        :param id_new: 新 ID
        """
        try:
            orig_hash = self.get_bundle_hash(orig_path, id_orig)
            new_hash = self.get_bundle_hash(new_path, id_new)

            # 备份原始加密文件
            self.file_backup(orig_hash)

            # 解密新文件 - 使用批量解密方法实际执行解密
            decrypted_new_paths = self._decrypt_dat_bundles_batch([new_hash])

            if not decrypted_new_paths:
                print("❌ Failed to decrypt new file")
                self._cleanup_temp_dirs()
                return

            decrypted_new_file = decrypted_new_paths[0]

            # 替换 ID
            edited_file = self.replace_file_path(decrypted_new_file, id_new, id_orig,
                                                 f"{EDITED_PATH}/{orig_hash}")

            # 加密修改后的文件
            encrypted_paths = self._encrypt_dat_bundles_batch([edited_file], [orig_hash])

            if not encrypted_paths:
                print("❌ Failed to encrypt file")
                self._cleanup_temp_dirs()
                return

            # 复制加密后的文件回游戏目录
            shutil.copyfile(encrypted_paths[0], self.get_bundle_path(orig_hash))
            print(f"✅ Replace completed: {orig_path} -> {new_path}")

        except Exception as e:
            print(f"❌ Error in replace_file_ids_with_encryption: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def replace_file_ids(self, orig_path: str, new_path: str, id_orig: str, id_new: str):
        """
        替换文件 ID (旧版本，不带加密处理)
        :param orig_path: 原始资源路径
        :param new_path: 新资源路径
        :param id_orig: 原始 ID
        :param id_new: 新 ID
        """
        orig_hash = self.get_bundle_hash(orig_path, id_orig)
        new_hash = self.get_bundle_hash(new_path, id_new)
        self.file_backup(orig_hash)
        edt_bundle_file_path = self.replace_file_path(self.get_bundle_path(new_hash), id_new, id_orig,
                                                      f"{EDITED_PATH}/{orig_hash}")
        shutil.copyfile(edt_bundle_file_path, self.get_bundle_path(orig_hash))

    def _replace_assets_batch(self, orig_paths: list, new_paths: list, id_orig: str, id_new: str,
                              asset_type: str = "asset"):
        """
        通用的批量资源替换方法（带加密/解密）
        :param orig_paths: 原始资源路径列表
        :param new_paths: 新资源路径列表
        :param id_orig: 原始 ID
        :param id_new: 新 ID
        :param asset_type: 资源类型名称（用于日志输出）
        """
        try:
            # 收集所有需要处理的 bundle hash
            bundle_info = []
            for i in range(len(orig_paths)):
                try:
                    orig_hash = self.get_bundle_hash(orig_paths[i], id_orig)
                    new_hash = self.get_bundle_hash(new_paths[i], id_new)
                    bundle_info.append((orig_hash, new_hash, orig_paths[i], new_paths[i]))
                except UmaFileNotFoundError as e:
                    print(f"⚠️  {e}")
                    continue

            if not bundle_info:
                print("❌ 没有找到需要处理的资源")
                return

            # 备份所有原始文件
            print(f"Backing up {len(bundle_info)} files...")
            for orig_hash, _, _, _ in bundle_info:
                try:
                    self.file_backup(orig_hash)
                except Exception as e:
                    print(f"⚠️  Failed to backup {orig_hash}: {e}")

            # 解密所有新文件
            new_hashes = [new_hash for _, new_hash, _, _ in bundle_info]
            decrypted_new_paths = self._decrypt_dat_bundles_batch(new_hashes)

            if not decrypted_new_paths:
                print("❌ 解密失败")
                self._cleanup_temp_dirs()
                return

            # 处理每个文件：替换 ID 并保存到 edited 目录
            print(f"Replacing IDs in {len(decrypted_new_paths)} files...")
            edited_files = []
            valid_bundle_indices = []  # 记录哪些bundle成功处理

            for i, decrypted_path in enumerate(decrypted_new_paths):
                try:
                    orig_hash, _, _, _ = bundle_info[i]
                    edited_file = self.replace_file_path(decrypted_path, id_new, id_orig,
                                                         f"{EDITED_PATH}/{orig_hash}")
                    edited_files.append(edited_file)
                    valid_bundle_indices.append(i)
                except Exception as e:
                    print(f"⚠️  Error replacing IDs in bundle {i}: {type(e).__name__}: {e}")
                    continue

            if not edited_files:
                print("❌ 没有文件被成功处理")
                self._cleanup_temp_dirs()
                return

            # 批量加密所有修改后的文件（只加密成功处理的文件）
            valid_orig_hashes = [bundle_info[i][0] for i in valid_bundle_indices]
            encrypted_paths = self._encrypt_dat_bundles_batch(edited_files, valid_orig_hashes)

            if not encrypted_paths:
                print("❌ 加密失败")
                self._cleanup_temp_dirs()
                return

            # 复制加密后的文件回游戏目录
            print(f"Copying {len(encrypted_paths)} files back to game directory...")
            for i, encrypted_path in enumerate(encrypted_paths):
                try:
                    bundle_index = valid_bundle_indices[i]
                    orig_hash, _, orig_path, new_path = bundle_info[bundle_index]
                    shutil.copyfile(encrypted_path, self.get_bundle_path(orig_hash))
                    print(f"✅ Replaced: {orig_path} -> {new_path}")
                except Exception as e:
                    print(f"⚠️  Error copying encrypted file back: {type(e).__name__}: {e}")

            # 清理临时目录
            self._cleanup_temp_dirs()
            print(f"✅ {asset_type.capitalize()} replacement completed: {id_orig} -> {id_new}")

        except Exception as e:
            print(f"❌ Error in {asset_type} replacement: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            self._cleanup_temp_dirs()

    def replace_body(self, id_orig: str, id_new: str):
        """
        替换身体（带加密/解密）- 批量处理
        :param id_orig: 原id, 例: 1046_01
        :param id_new: 新id
        """
        self._replace_assets_batch(assets_path.get_body_path(id_orig), assets_path.get_body_path(id_new), id_orig,
                                   id_new, "body")

    def replace_head(self, id_orig: str, id_new: str):
        """
        替换头部（带加密/解密）- 批量处理
        :param id_orig: 原id, 例: 1046_01
        :param id_new: 新id
        """
        self._replace_assets_batch(assets_path.get_head_path(id_orig), assets_path.get_head_path(id_new), id_orig,
                                   id_new, "head")

    def replace_tail(self, id_orig: str, id_new: str):
        """
        替换尾巴（带加密/解密）- 批量处理
        目前无法跨模型更换尾巴，更换目标不能和原马娘同时出场。
        :param id_orig: 原id, 例: 1046
        :param id_new: 新id
        """

        def check_vaild_path(paths: list):
            try:
                self.get_bundle_hash(paths[0], None)
            except UmaFileNotFoundError:
                return False
            return True

        orig_paths1 = assets_path.get_tail1_path(id_orig)
        orig_paths2 = assets_path.get_tail2_path(id_orig)

        new_paths1 = assets_path.get_tail1_path(id_new)
        new_paths2 = assets_path.get_tail2_path(id_new)

        orig_paths = None
        new_paths = None
        use_id1 = -1
        use_id2 = -1
        if check_vaild_path(orig_paths1):
            orig_paths = orig_paths1
            use_id1 = 1
        if check_vaild_path(orig_paths2):
            orig_paths = orig_paths2
            use_id1 = 2
        if check_vaild_path(new_paths1):
            new_paths = new_paths1
            use_id2 = 1
        if check_vaild_path(new_paths2):
            use_id2 = 2
            new_paths = new_paths2

        if (orig_paths is None) or (new_paths is None):
            print("tail not found")
            return

        if use_id1 != use_id2:
            print(f"{id_orig} 模型编号: {use_id1}, {id_new} 模型编号: {use_id2}, 目前无法跨模型修改尾巴。")
            return

        print("注意, 更换尾巴后, 更换目标不能和原马娘同时出场。")

        # 使用通用的批量替换方法
        self._replace_assets_batch(orig_paths, new_paths, id_orig, id_new, "tail")

    def edit_gac_chr_start(self, dress_id: str, type: str):
        """
        替换开门人物
        :param dress_id: 目标开门id, 例: 100101
        :param type: 001骏川手纲，002秋川弥生
        """
        try:
            path = assets_path.get_gac_chr_start_path(type)
            orig_hash = self.get_bundle_hash(path, None)

            # 备份原始文件
            self.file_backup(orig_hash)

            # 解密文件
            decrypted_paths = self._decrypt_dat_bundles_batch([orig_hash])

            if not decrypted_paths:
                print("❌ Failed to decrypt bundle")
                return

            decrypted_path = decrypted_paths[0]

            # 加载并修改
            env = UnityPy.load(decrypted_path)

            for obj in env.objects:
                if obj.type.name == "MonoBehaviour":
                    if obj.serialized_type.nodes:
                        tree = obj.read_typetree()
                        if "runtime_gac_chr_start_00" in tree["m_Name"]:
                            tree["_characterList"][0]["_characterKeys"]["_selectCharaId"] = int(dress_id[:-2])
                            tree["_characterList"][0]["_characterKeys"]["_selectClothId"] = int(dress_id)
                            obj.save_typetree(tree)
                            print(f"✅ Updated gac_chr_start: CharaId={dress_id[:-2]}, ClothId={dress_id}")

            # 保存修改后的文件
            edited_file = f"{EDITED_PATH}/{orig_hash}"
            os.makedirs(os.path.dirname(edited_file), exist_ok=True)

            with open(edited_file, "wb") as f:
                f.write(env.file.save())

            # 加密修改后的文件
            encrypted_paths = self._encrypt_dat_bundles_batch([edited_file], [orig_hash])

            if not encrypted_paths:
                print("❌ Failed to encrypt bundle")
                return

            # 复制加密后的文件回游戏目录
            shutil.copyfile(encrypted_paths[0], self.get_bundle_path(orig_hash))
            print(f"✅ Gac chr start replacement completed: {dress_id}")

        except Exception as e:
            print(f"❌ Error in edit_gac_chr_start: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def edit_cutin_skill(self, id_orig: str, id_target: str):
        """
        替换技能
        :param id_orig: 原id, 例: 100101
        :param id_target: 新id
        """
        try:
            # 获取目标技能和原始技能的路径
            target_path = assets_path.get_cutin_skill_path(id_target)
            orig_path = assets_path.get_cutin_skill_path(id_orig)

            target_hash = self.get_bundle_hash(target_path, None)
            orig_hash = self.get_bundle_hash(orig_path, None)

            # 备份原始文件
            self.file_backup(orig_hash)

            # 一次性解密两个文件（目标和原始）
            decrypted_paths = self._decrypt_dat_bundles_batch([target_hash, orig_hash])

            if len(decrypted_paths) < 2:
                print("❌ Failed to decrypt bundles")
                self._cleanup_temp_dirs()
                return

            target_decrypted_path = decrypted_paths[0]
            orig_decrypted_path = decrypted_paths[1]

            # 加载目标文件并提取数据
            target = UnityPy.load(target_decrypted_path)

            target_tree = None
            target_clothe_id = None
            target_cy_spring_name_list = None

            for obj in target.objects:
                if obj.type.name == "MonoBehaviour":
                    if obj.serialized_type.nodes:
                        tree = obj.read_typetree()
                        if "runtime_crd1" in tree["m_Name"]:
                            target_tree = tree
                            for character in tree["_characterList"]:
                                target_clothe_id = str(character["_characterKeys"]["_selectClothId"])

            if target_tree is None:
                print("❌ Target data cannot be parsed")
                self._cleanup_temp_dirs()
                return

            for character in target_tree["_characterList"]:
                for targetList in character["_characterKeys"]["thisList"]:
                    if len(targetList["_enableCySpringList"]) > 0:
                        target_cy_spring_name_list = targetList["_targetCySpringNameList"]

            # 加载并修改原始文件
            env = UnityPy.load(orig_decrypted_path)

            for obj in env.objects:
                if obj.type.name == "MonoBehaviour":
                    if obj.serialized_type.nodes:
                        tree = obj.read_typetree()
                        if "runtime_crd1" in tree["m_Name"]:
                            for character in tree["_characterList"]:
                                character["_characterKeys"]["_selectCharaId"] = int(target_clothe_id[:-2])
                                character["_characterKeys"]["_selectClothId"] = int(target_clothe_id)
                                character["_characterKeys"]["_selectHeadId"] = 0
                                for outputList in character["_characterKeys"]["thisList"]:
                                    if len(outputList["_enableCySpringList"]) > 0:
                                        outputList["_enableCySpringList"] = [1] * len(target_cy_spring_name_list)
                                        outputList["_targetCySpringNameList"] = target_cy_spring_name_list
                            obj.save_typetree(tree)
                            print(f"✅ Updated skill data: CharaId={target_clothe_id[:-2]}, ClothId={target_clothe_id}")

            # 保存修改后的文件
            edited_file = f"{EDITED_PATH}/{orig_hash}"
            os.makedirs(os.path.dirname(edited_file), exist_ok=True)

            with open(edited_file, "wb") as f:
                f.write(env.file.save())

            # 加密修改后的文件
            encrypted_paths = self._encrypt_dat_bundles_batch([edited_file], [orig_hash])

            if not encrypted_paths:
                print("❌ Failed to encrypt bundle")
                self._cleanup_temp_dirs()
                return

            # 复制加密后的文件回游戏目录
            shutil.copyfile(encrypted_paths[0], self.get_bundle_path(orig_hash))
            print(f"✅ Skill replacement completed: {id_orig} -> {id_target}")

        except UmaFileNotFoundError as e:
            print(f"❌ {e}")
        except Exception as e:
            print(f"❌ Error in edit_cutin_skill: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def replace_race_result(self, id_orig: str, id_new: str):
        """
        替换G1胜利动作（带加密/解密）- 批量处理
        :param id_orig: 原id, 例: 100101
        :param id_new: 新id
        """
        try:
            orig_paths = assets_path.get_crd_race_result_path(id_orig)
            new_paths = assets_path.get_crd_race_result_path(id_new)

            # 收集所有需要处理的 bundle hash
            bundle_info = []  # 存储 (orig_hash, new_hash, orig_path, new_path)
            for i in range(len(orig_paths)):
                try:
                    orig_hash = self.get_bundle_hash(orig_paths[i], id_orig)
                    new_hash = self.get_bundle_hash(new_paths[i], id_new)
                    bundle_info.append((orig_hash, new_hash, orig_paths[i], new_paths[i]))
                except UmaFileNotFoundError as e:
                    print(f"⚠️  {e}")

            if not bundle_info:
                print("没有找到需要处理的资源")
                return

            # 备份所有原始文件
            print(f"Backing up {len(bundle_info)} files...")
            for orig_hash, _, _, _ in bundle_info:
                self.file_backup(orig_hash)

            # 解密所有新文件
            new_hashes = [new_hash for _, new_hash, _, _ in bundle_info]
            decrypted_new_paths = self._decrypt_dat_bundles_batch(new_hashes)

            if not decrypted_new_paths:
                print("❌ 解密失败")
                return

            # 处理每个文件：替换 ID 并保存到 edited 目录
            print(f"Replacing IDs in {len(decrypted_new_paths)} files...")
            edited_files = []
            for i, decrypted_path in enumerate(decrypted_new_paths):
                orig_hash, _, _, _ = bundle_info[i]
                edited_file = self.replace_file_path(decrypted_path, id_new, id_orig,
                                                     f"{EDITED_PATH}/{orig_hash}")
                edited_files.append(edited_file)

            # 批量加密所有修改后的文件
            orig_hashes = [orig_hash for orig_hash, _, _, _ in bundle_info]
            encrypted_paths = self._encrypt_dat_bundles_batch(edited_files, orig_hashes)

            if not encrypted_paths:
                print("❌ 加密失败")
                return

            # 复制加密后的文件回游戏目录
            print(f"Copying {len(encrypted_paths)} files back to game directory...")
            for i, encrypted_path in enumerate(encrypted_paths):
                orig_hash, _, orig_path, new_path = bundle_info[i]
                shutil.copyfile(encrypted_path, self.get_bundle_path(orig_hash))
                print(f"✅ Replaced: {orig_path} -> {new_path}")

            # 清理临时目录
            self._cleanup_temp_dirs()
            print(f"✅ Race result replacement completed: {id_orig} -> {id_new}")

        except Exception as e:
            print(f"❌ Error in replace_race_result: {e}")
            import traceback
            traceback.print_exc()
            self._cleanup_temp_dirs()

    def unlock_live_dress(self):

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        def get_all_dress_in_table():
            self.master_conn.row_factory = dict_factory
            cursor = self.master_conn.cursor()
            cursor.execute("SELECT * FROM dress_data")
            # fetchall as result
            query = cursor.fetchall()
            # close connection
            cursor.close()
            return query

        def get_unique_in_table():
            self.conn.row_factory = dict_factory
            cursor = self.conn.cursor()
            cursor.execute("SELECT n FROM a WHERE n like '%pfb_chr1____90'")
            # fetchall as result
            names = cursor.fetchall()
            # close connection
            cursor.close()
            list = []
            for name in names:
                list.append(name["n"][-7:-3])
            return list

        def create_data(dress, unique):
            dress['id'] = dress['id'] + 89
            dress['body_type_sub'] = 90
            if str(dress['id'])[:-2] in set(unique):
                dress['head_sub_id'] = 90
            else:
                dress['head_sub_id'] = 0
            self.master_conn.row_factory = dict_factory
            cursor = self.master_conn.cursor()
            cursor.execute("INSERT INTO dress_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           [dress['id'], dress['condition_type'], dress['have_mini'], dress['general_purpose'],
                            dress['costume_type'], dress['chara_id'], dress['use_gender'], dress['body_shape'],
                            dress['body_type'], dress['body_type_sub'], dress['body_setting'], dress['use_race'],
                            dress['use_live'], dress['use_live_theater'], dress['use_home'], dress['use_dress_change'],
                            dress['is_wet'], dress['is_dirt'], dress['head_sub_id'], dress['use_season'],
                            dress['dress_color_main'], dress['dress_color_sub'], dress['color_num'],
                            dress['disp_order'],
                            dress['tail_model_id'], dress['tail_model_sub_id'], dress['mini_mayu_shader_type'],
                            dress['start_time'], dress['end_time']])
            self.master_conn.commit()
            cursor.close()

        def unlock_data():
            self.master_conn.row_factory = dict_factory
            cursor = self.master_conn.cursor()
            cursor.execute("UPDATE dress_data SET use_live = 1, use_live_theater = 1")
            self.master_conn.commit()
            cursor.close()

        dresses = get_all_dress_in_table()
        unique = get_unique_in_table()
        for dress in dresses:
            if 100000 < dress['id'] < 200000 and str(dress['id']).endswith('01'):
                create_data(dress, unique)
        unlock_data()

    def clear_live_blur(self, edit_id: str):
        cursor = self.conn.cursor()
        query = cursor.execute("SELECT h, n FROM a WHERE n LIKE 'cutt/cutt_son%/son%_camera'").fetchall()
        bundle_names = [i[0] for i in query]
        path_names = [i[1] for i in query]
        cursor.close()
        target_path = f"cutt/cutt_son{edit_id}/son{edit_id}_camera" if edit_id != "" else None
        tLen = len(bundle_names)

        # 先收集所有需要处理的 bundle hash
        bundles_to_process = []
        for n, bn in enumerate(bundle_names):
            path_name = path_names[n]
            if target_path is not None:
                if path_name != target_path:
                    continue
            bundles_to_process.append((n, bn, path_name))

        if not bundles_to_process:
            print("No bundles to process")
            return

        # 备份所有原始文件
        try:
            print(f"Backing up {len(bundles_to_process)} files...")
            for _, bn, _ in bundles_to_process:
                self.file_backup(bn)

            # 解密所有需要处理的 bundle
            bundle_hashes = [bn for _, bn, _ in bundles_to_process]
            decrypted_paths = self._decrypt_dat_bundles_batch(bundle_hashes)

            if not decrypted_paths:
                print("❌ Failed to decrypt bundles")
                self._cleanup_temp_dirs()
                return

            # 处理每个解密后的文件
            print(f"Processing {len(decrypted_paths)} bundles...")
            edited_files = []
            for i, decrypted_path in enumerate(decrypted_paths):
                n, bn, path_name = bundles_to_process[i]
                print(f"Editing: {path_name} ({n + 1}/{tLen})")
                try:
                    env = UnityPy.load(decrypted_path)
                    for obj in env.objects:
                        if obj.type.name == "MonoBehaviour":
                            if not obj.serialized_type.nodes:
                                continue
                            tree = obj.read_typetree()

                            tree['postEffectDOFKeys']['thisList'] = [tree['postEffectDOFKeys']['thisList'][0]]
                            dof_set_data = {
                                "frame": 0,
                                "attribute": 327680,
                                "interpolateType": 0,
                                "curve": {
                                    "m_Curve": [],
                                    "m_PreInfinity": 2,
                                    "m_PostInfinity": 2,
                                    "m_RotationOrder": 4
                                },
                                "easingType": 0,
                                "forcalSize": 30.0,
                                "blurSpread": 20.0,
                                "charactor": 1,
                                "dofBlurType": 3,
                                "dofQuality": 1,
                                "dofForegroundSize": 0.0,
                                "dofFgBlurSpread": 1.0,
                                "dofFocalPoint": 1.0,
                                "dofSmoothness": 1.0,
                                "BallBlurPowerFactor": 0.0,
                                "BallBlurBrightnessThreshhold": 0.0,
                                "BallBlurBrightnessIntensity": 1.0,
                                "BallBlurSpread": 0.0
                            }
                            for k in dof_set_data:
                                tree['postEffectDOFKeys']['thisList'][0][k] = dof_set_data[k]

                            tree['postEffectBloomDiffusionKeys']['thisList'] = []
                            tree['radialBlurKeys']['thisList'] = []

                            obj.save_typetree(tree)

                    # 保存修改后的文件
                    edited_file = f"{EDITED_PATH}/{bn}"
                    os.makedirs(os.path.dirname(edited_file), exist_ok=True)

                    with open(edited_file, "wb") as f:
                        f.write(env.file.save())

                    edited_files.append(edited_file)
                    print(f"✅ Edited: {path_name}")

                except Exception as e:
                    print(f"❌ Exception occurred when editing file: {bn}\n{e}")

            # 批量加密所有修改后的文件
            encrypted_paths = self._encrypt_dat_bundles_batch(edited_files, bundle_hashes)

            if not encrypted_paths:
                print("❌ Failed to encrypt bundles")
                self._cleanup_temp_dirs()
                return

            # 复制加密后的文件回游戏目录
            print(f"Copying {len(encrypted_paths)} files back to game directory...")
            for i, encrypted_path in enumerate(encrypted_paths):
                bn = bundles_to_process[i][1]
                shutil.copyfile(encrypted_path, self.get_bundle_path(bn))
                print(f"✅ Updated: {bn}")

            print("✅ Clear live blur completed")

        except Exception as e:
            print(f"❌ Error in clear_live_blur: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def save_char_body_texture(self, char_id: str, force: bool = False) -> t.Tuple[bool, str]:
        """
        导出角色身体纹理到本地目录
        :param char_id: 角色ID，例: 1001_00
        :param force: 是否强制覆盖已存���的文件
        :return: (是否为新导出, 导出目录路径)
        """
        try:
            char_code = char_id[:4]
            export_dir = f"{EDITED_PATH}/textures/body_{char_code}"

            # 检查目录是否已存在
            is_not_exist = not os.path.isdir(export_dir)

            if not is_not_exist and not force:
                return (False, export_dir)

            # 如果强制覆盖，删除旧目录
            if not is_not_exist and force:
                shutil.rmtree(export_dir)

            os.makedirs(export_dir, exist_ok=True)

            # 获取身体材质路径（使用完整的char_id，包括后缀）
            mtl_path = assets_path.get_body_mtl_path(char_id)
            mtl_names = assets_path.get_body_mtl_names(char_id)

            print(f"Exporting body textures for character {char_id}...")

            try:
                bundle_hash = self.get_bundle_hash(mtl_path, char_id)

                # 备份原始文件
                self.file_backup(bundle_hash)

                # 解密bundle文件
                decrypted_paths = self._decrypt_dat_bundles_batch([bundle_hash])

                if not decrypted_paths:
                    print("❌ Failed to decrypt bundle")
                    return (True, export_dir)

                decrypted_path = decrypted_paths[0]

                # 从解密后的文件加载并提取纹理
                env = UnityPy.load(decrypted_path)

                texture_count = 0
                actual_texture_names = []
                for obj in env.objects:
                    if obj.type.name == "Texture2D":
                        texture_count += 1
                        try:
                            data = obj.read()
                            if hasattr(data, "m_Name"):
                                texture_name = data.m_Name
                                actual_texture_names.append(texture_name)
                                # 导出纹理为PNG - 不检查名称，全部导出
                                img_path = f"{export_dir}/{texture_name}.png"
                                try:
                                    img = data.image
                                    img.save(img_path)
                                    print(f"✅ Exported: {texture_name}")
                                except Exception as e:
                                    print(f"❌ Failed to save image {texture_name}: {e}")
                        except Exception as e:
                            print(f"❌ Error reading texture object: {e}")

                print(f"Total Texture2D objects found: {texture_count}")
                print(f"Actual texture names: {actual_texture_names}")

            except UmaFileNotFoundError as e:
                print(f"⚠️  {e}")
            except Exception as e:
                print(f"⚠️  Error exporting texture: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # 清理临时目录
                self._cleanup_temp_dirs()

            print(f"✅ Body textures exported to: {export_dir}")
            return (True, export_dir)

        except Exception as e:
            print(f"❌ Error in save_char_body_texture: {e}")
            raise

    def replace_char_body_texture(self, char_id: str):
        """
        替换角色身体纹理（从本地修改后的纹理）
        :param char_id: 角色ID，例: 1001_00
        """
        try:
            char_code = char_id[:4]
            export_dir = f"{EDITED_PATH}/textures/body_{char_code}"

            if not os.path.isdir(export_dir):
                print(f"❌ Texture directory not found: {export_dir}")
                return

            # 获取身体材质路径（使用完整的char_id，包括后缀）
            mtl_path = assets_path.get_body_mtl_path(char_id)
            mtl_names = assets_path.get_body_mtl_names(char_id)

            try:
                bundle_hash = self.get_bundle_hash(mtl_path, char_id)

                # 解密纹理文件
                decrypted_paths = self._decrypt_dat_bundles_batch([bundle_hash])

                if not decrypted_paths:
                    print("❌ Failed to decrypt textures")
                    return

                decrypted_path = decrypted_paths[0]

                # 加载并替换纹理
                print(f"Replacing textures in bundle...")
                env = UnityPy.load(decrypted_path)

                textures_updated = 0
                for obj in env.objects:
                    if obj.type.name == "Texture2D":
                        data = obj.read()
                        if hasattr(data, "m_Name") and data.m_Name in mtl_names:
                            texture_name = data.m_Name
                            file_path = f"{export_dir}/{texture_name}.png"

                            if os.path.isfile(file_path):
                                try:
                                    # 加载修改后的纹理
                                    image = Image.open(file_path)
                                    data.image = image
                                    data.save()
                                    textures_updated += 1
                                    print(f"✅ Updated texture: {texture_name}")
                                except Exception as e:
                                    print(f"❌ Failed to update texture {texture_name}: {type(e).__name__}: {e}")

                if textures_updated == 0:
                    print("⚠️  No textures were updated")
                else:
                    print(f"✅ Total textures updated: {textures_updated}")

                edited_file = f"{EDITED_PATH}/{bundle_hash}"
                os.makedirs(os.path.dirname(edited_file), exist_ok=True)

                with open(edited_file, "wb") as f:
                    f.write(env.file.save())

                # 加密修改后的文件
                encrypted_paths = self._encrypt_dat_bundles_batch([edited_file], [bundle_hash])

                if not encrypted_paths:
                    print("❌ Failed to encrypt textures")
                    return

                # 复制加密后的文件回游戏目录
                print(f"Copying texture bundle back to game directory...")
                shutil.copyfile(encrypted_paths[0], self.get_bundle_path(bundle_hash))
                print(f"✅ Updated bundle: {bundle_hash}")

                print(f"✅ Body texture replacement completed: {char_id}")

            except UmaFileNotFoundError as e:
                print(f"⚠️  {e}")

        except Exception as e:
            print(f"❌ Error in replace_char_body_texture: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def save_char_head_texture(self, char_id: str, force: bool = False):
        """
        导出角色头部纹理到本地目录（生成器）
        :param char_id: 角色ID，例: 1001_00
        :param force: 是否强制覆盖已存在的文件
        :return: 生成器，每次yield (是否为新导出, 导出目录路径)
        """
        try:
            char_code = char_id[:4]
            export_dir = f"{EDITED_PATH}/textures/head_{char_code}"

            # 检查目录是否已存在
            is_not_exist = not os.path.isdir(export_dir)

            if not is_not_exist and not force:
                yield (False, export_dir)
                return

            # 如果强制覆盖，删除旧目录
            if not is_not_exist and force:
                shutil.rmtree(export_dir)

            os.makedirs(export_dir, exist_ok=True)

            # 获取头部材质路径（使用完整的char_id，包括后缀）
            mtl_paths = assets_path.get_head_mtl_path(char_id)

            print(f"Exporting head textures for character {char_id}...")

            # 收集所有需要处理的 bundle hash
            bundle_hashes = []
            for mtl_path in mtl_paths:
                try:
                    bundle_hash = self.get_bundle_hash(mtl_path, char_id)
                    bundle_hashes.append((bundle_hash, mtl_path))
                    # 备份原始文件
                    self.file_backup(bundle_hash)
                except UmaFileNotFoundError as e:
                    print(f"⚠️  {e}")

            if not bundle_hashes:
                print("❌ No texture bundles found")
                yield (True, export_dir)
                return

            # 解密所有头部纹理文件
            hashes_only = [h for h, _ in bundle_hashes]
            decrypted_paths = self._decrypt_dat_bundles_batch(hashes_only)

            if not decrypted_paths:
                print("❌ Failed to decrypt bundles")
                yield (True, export_dir)
                return

            # 从解密后的文件加载并提取纹理
            for i, decrypted_path in enumerate(decrypted_paths):
                try:
                    env = UnityPy.load(decrypted_path)
                    for obj in env.objects:
                        if obj.type.name == "Texture2D":
                            data = obj.read()
                            if hasattr(data, "m_Name"):
                                texture_name = data.m_Name
                                # 导出纹理为PNG
                                img_path = f"{export_dir}/{texture_name}.png"
                                img = data.image
                                img.save(img_path)
                                print(f"✅ Exported: {texture_name}")
                except Exception as e:
                    print(f"⚠️  Error processing bundle {i}: {e}")

            print(f"✅ Head textures exported to: {export_dir}")
            yield (True, export_dir)

        except Exception as e:
            print(f"❌ Error in save_char_head_texture: {e}")
            raise
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def replace_char_head_texture(self, char_id: str):
        """
        替换角色头部纹理（从本地修改后的纹理）
        :param char_id: 角色ID，例: 1001_00
        """
        try:
            char_code = char_id[:4]
            export_dir = f"{EDITED_PATH}/textures/head_{char_code}"

            if not os.path.isdir(export_dir):
                print(f"❌ Texture directory not found: {export_dir}")
                return

            # 获取头部材质路径（使用完整的char_id，包括后缀）
            mtl_paths = assets_path.get_head_mtl_path(char_id)

            # 收集所有需要处理的 bundle hash
            bundle_info = []
            for mtl_path in mtl_paths:
                try:
                    bundle_hash = self.get_bundle_hash(mtl_path, char_id)
                    bundle_info.append((bundle_hash, mtl_path))
                except UmaFileNotFoundError as e:
                    print(f"⚠️  {e}")
                    continue

            if not bundle_info:
                print("❌ No texture bundles found")
                return

            # 解密所有纹理文件
            bundle_hashes = [bundle_hash for bundle_hash, _ in bundle_info]
            decrypted_paths = self._decrypt_dat_bundles_batch(bundle_hashes)

            if not decrypted_paths:
                print("❌ Failed to decrypt textures")
                return

            # 处理每个纹理文件
            print(f"Replacing textures in {len(decrypted_paths)} bundles...")
            edited_files = []
            valid_bundle_indices = []  # 记录哪些bundle处理成功
            for i, decrypted_path in enumerate(decrypted_paths):
                try:
                    env = UnityPy.load(decrypted_path)
                    textures_updated = 0

                    for obj in env.objects:
                        if obj.type.name == "Texture2D":
                            data = obj.read()
                            if hasattr(data, "m_Name"):
                                texture_name = data.m_Name
                                file_path = f"{export_dir}/{texture_name}.png"

                                if os.path.isfile(file_path):
                                    try:
                                        # 加载修改后的纹理
                                        image = Image.open(file_path)
                                        data.image = image
                                        data.save()
                                        textures_updated += 1
                                        print(f"✅ Updated texture: {texture_name}")
                                    except Exception as e:
                                        print(f"❌ Failed to update texture {texture_name}: {type(e).__name__}: {e}")

                    if textures_updated == 0:
                        print(f"⚠️  No textures updated in bundle {i}")

                    bundle_hash = bundle_info[i][0]
                    edited_file = f"{EDITED_PATH}/{bundle_hash}"
                    os.makedirs(os.path.dirname(edited_file), exist_ok=True)

                    with open(edited_file, "wb") as f:
                        f.write(env.file.save())

                    edited_files.append(edited_file)
                    valid_bundle_indices.append(i)

                except Exception as e:
                    print(f"⚠️  Error processing texture bundle {i}: {type(e).__name__}: {e}")
                    continue

            if not edited_files:
                print("❌ No texture bundles were successfully processed")
                return

            # 批量加密所有修改后的文件（只加密成功处理的文件）
            valid_bundle_hashes = [bundle_hashes[i] for i in valid_bundle_indices]
            encrypted_paths = self._encrypt_dat_bundles_batch(edited_files, valid_bundle_hashes)

            if not encrypted_paths:
                print("❌ Failed to encrypt textures")
                return

            # 复制加密后的文件回游戏目录
            print(f"Copying {len(encrypted_paths)} texture bundles back to game directory...")
            for i, encrypted_path in enumerate(encrypted_paths):
                original_bundle_index = valid_bundle_indices[i]
                bundle_hash, _ = bundle_info[original_bundle_index]
                try:
                    shutil.copyfile(encrypted_path, self.get_bundle_path(bundle_hash))
                    print(f"✅ Updated bundle: {bundle_hash}")
                except Exception as e:
                    print(f"⚠️  Failed to copy bundle back: {type(e).__name__}: {e}")

            print(f"✅ Head texture replacement completed: {char_id}")

        except Exception as e:
            print(f"❌ Error in replace_char_head_texture: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def get_texture_in_bundle(self, bundle_hash: str, texture_names: list, force: bool = False) -> t.Tuple[bool, str]:
        """
        从 bundle 中导出指定的 Texture2D 资源
        :param bundle_hash: bundle 的 hash
        :param texture_names: 要导出的纹理名称列表
        :param force: 是否强制覆盖已存在的文件
        :return: (是否为新导出, 导出目录路径)
        """
        try:
            export_dir = f"{EDITED_PATH}/textures/custom_{bundle_hash}"

            # 检查目录是否已存在
            is_not_exist = not os.path.isdir(export_dir)

            if not is_not_exist and not force:
                return (False, export_dir)

            # 如果强制覆盖，删除旧目录
            if not is_not_exist and force:
                shutil.rmtree(export_dir)

            os.makedirs(export_dir, exist_ok=True)

            print(f"Exporting textures from bundle {bundle_hash}...")

            # 备份原始文件
            self.file_backup(bundle_hash)

            # 解密bundle文件
            decrypted_paths = self._decrypt_dat_bundles_batch([bundle_hash])

            if not decrypted_paths:
                print("❌ Failed to decrypt bundle")
                return (True, export_dir)

            decrypted_path = decrypted_paths[0]

            # 从解密后的文件加载并提取纹理
            env = UnityPy.load(decrypted_path)

            texture_count = 0
            for obj in env.objects:
                if obj.type.name == "Texture2D":
                    try:
                        data = obj.read()
                        if hasattr(data, "m_Name"):
                            texture_name = data.m_Name
                            # 如果指定了纹理名称列表，只导出匹配的纹理；否则导出所有纹理
                            if not texture_names or texture_name in texture_names:
                                img_path = f"{export_dir}/{texture_name}.png"
                                try:
                                    img = data.image
                                    img.save(img_path)
                                    texture_count += 1
                                    print(f"✅ Exported: {texture_name}")
                                except Exception as e:
                                    print(f"❌ Failed to save image {texture_name}: {e}")
                    except Exception as e:
                        print(f"❌ Error reading texture object: {e}")

            print(f"Total textures exported: {texture_count}")
            print(f"✅ Textures exported to: {export_dir}")
            return (True, export_dir)

        except Exception as e:
            print(f"❌ Error in get_texture_in_bundle: {e}")
            raise
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()

    def replace_texture2d(self, bundle_hash: str):
        """
        替换 bundle 中的 Texture2D 资源（从本地修改后的纹理）
        :param bundle_hash: bundle 的 hash
        :return: 编辑后的文件路径
        """
        try:
            export_dir = f"{EDITED_PATH}/textures/custom_{bundle_hash}"

            if not os.path.isdir(export_dir):
                print(f"❌ Texture directory not found: {export_dir}")
                return None

            # 解密纹理文件
            decrypted_paths = self._decrypt_dat_bundles_batch([bundle_hash])

            if not decrypted_paths:
                print("❌ Failed to decrypt textures")
                return None

            decrypted_path = decrypted_paths[0]

            # 加载并替换纹理
            print(f"Replacing textures in bundle {bundle_hash}...")
            env = UnityPy.load(decrypted_path)

            textures_updated = 0
            for obj in env.objects:
                if obj.type.name == "Texture2D":
                    data = obj.read()
                    if hasattr(data, "m_Name"):
                        texture_name = data.m_Name
                        file_path = f"{export_dir}/{texture_name}.png"

                        if os.path.isfile(file_path):
                            try:
                                # 加载修改后的纹理
                                image = Image.open(file_path)
                                data.image = image
                                data.save()
                                textures_updated += 1
                                print(f"✅ Updated texture: {texture_name}")
                            except Exception as e:
                                print(f"❌ Failed to update texture {texture_name}: {type(e).__name__}: {e}")

            if textures_updated == 0:
                print("⚠️  No textures were updated")
            else:
                print(f"✅ Total textures updated: {textures_updated}")

            # 保存修改后的文件（未加密）
            edited_file = f"{EDITED_PATH}/{bundle_hash}"
            os.makedirs(os.path.dirname(edited_file), exist_ok=True)

            with open(edited_file, "wb") as f:
                f.write(env.file.save())

            print(f"✅ Texture replacement completed for bundle: {bundle_hash}")
            return edited_file

        except Exception as e:
            print(f"❌ Error in replace_texture2d: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            # 清理临时目录
            self._cleanup_temp_dirs()
