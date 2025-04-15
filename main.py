#!/usr/bin/env python3
"""
MySQL每日滚动备份到阿里云OSS一体化脚本
需安装：pip install oss2 python-dotenv
"""

import gzip
import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import oss2
from dotenv import load_dotenv

# 初始化配置
load_dotenv()  # 从.env文件加载配置

CONFIG = {
    # MySQL配置
    "mysql_user": os.getenv("MYSQL_USER"),
    "mysql_password": os.getenv("MYSQL_PASSWORD"),
    "mysql_database": os.getenv("MYSQL_DATABASE"),
    "mysql_host": os.getenv("MYSQL_HOST", "localhost"),
    "mysql_port": os.getenv("MYSQL_PORT", "3306"),

    # 备份配置
    "mysqldump_path": Path(os.getenv("MYSQLDUMP_PATH", "mysqldump")),
    "local_backup_dir": Path(os.getenv("LOCAL_BACKUP_DIR", "/backups")),
    "keep_local_days": int(os.getenv("KEEP_LOCAL_DAYS", 3)),

    # OSS配置
    "oss_endpoint": os.getenv("OSS_ENDPOINT"),
    "oss_bucket": os.getenv("OSS_BUCKET"),
    "oss_prefix": os.getenv("OSS_PREFIX", "mysql-backups/"),
    "keep_oss_days": int(os.getenv("KEEP_OSS_DAYS", 30)),
}

# 初始化日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("mysql_backup.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BackupManager:
    def __init__(self):
        self.today = datetime.now().strftime("%Y%m%d")
        self.local_dir = CONFIG["local_backup_dir"]
        self.local_dir.mkdir(parents=True, exist_ok=True)

        # 初始化OSS客户端
        auth = oss2.Auth(
            os.getenv("OSS_ACCESS_KEY_ID"),
            os.getenv("OSS_ACCESS_KEY_SECRET")
        )
        self.bucket = oss2.Bucket(
            auth,
            CONFIG["oss_endpoint"],
            CONFIG["oss_bucket"]
        )

    def run_mysqldump(self):
        """执行MySQL备份并压缩"""
        dump_file = self.local_dir / f"{CONFIG['mysql_database']}_{self.today}.sql"
        gz_file = dump_file.with_suffix(".sql.gz")

        try:
            # 使用mysqldump导出数据
            with dump_file.open("wb") as f:
                cmd = [
                    CONFIG["mysqldump_path"],
                    "--single-transaction",
                    "-h", CONFIG["mysql_host"],
                    "-P", CONFIG["mysql_port"],
                    "-u", CONFIG["mysql_user"],
                    f"-p{CONFIG['mysql_password']}",
                    CONFIG["mysql_database"]
                ]
                process = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE)

                if process.returncode != 0:
                    raise RuntimeError(f"mysqldump失败: {process.stderr.decode()}")

            # 压缩备份文件
            with dump_file.open("rb") as f_in, gz_file.open("wb") as f_out:
                with gzip.GzipFile(fileobj=f_out, mode="wb") as gz:
                    gz.write(f_in.read())

            dump_file.unlink()  # 删除未压缩文件
            logger.info(f"本地备份成功: {gz_file}")
            return gz_file

        except Exception as e:
            logger.error(f"备份失败: {str(e)}")
            if dump_file.exists():
                dump_file.unlink()
            raise

    def upload_to_oss(self, file_path):
        """上传备份到OSS，需要oss::putObject权限"""
        object_name = f"{CONFIG['oss_prefix']}{file_path.name}"

        try:
            self.bucket.put_object_from_file(object_name, str(file_path))
            logger.info(f"OSS上传成功: {object_name}")
        except oss2.exceptions.OssError as e:
            logger.error(f"OSS上传失败: {str(e)}")
            raise

    def clean_local_backups(self):
        """清理本地旧备份"""
        cutoff_date = datetime.now() - timedelta(days=CONFIG["keep_local_days"])

        for f in self.local_dir.glob("*.sql.gz"):
            file_date_str = f.name.split("_")[-1].replace(".sql.gz", "")
            file_date = datetime.strptime(file_date_str, "%Y%m%d")

            if file_date < cutoff_date:
                try:
                    f.unlink()
                    logger.info(f"已删除本地备份: {f.name}")
                except Exception as e:
                    logger.error(f"删除本地文件失败: {str(e)}")

    def clean_oss_backups(self):
        """清理OSS旧备份，需要oss::listObjects和oss::deleteObject权限，安全起见，可使用OSS生命周期功能删除旧备份"""
        cutoff_date = datetime.now() - timedelta(days=CONFIG["keep_oss_days"])

        for obj in oss2.ObjectIterator(self.bucket, prefix=CONFIG["oss_prefix"]):
            if not obj.key.endswith(".sql.gz"):
                continue

            try:
                file_date_str = Path(obj.key).name.split("_")[-1].replace(".sql.gz", "")
                file_date = datetime.strptime(file_date_str, "%Y%m%d")

                if file_date < cutoff_date:
                    self.bucket.delete_object(obj.key)
                    logger.info(f"已删除OSS备份: {obj.key}")
            except Exception as e:
                logger.error(f"处理OSS文件失败 {obj.key}: {str(e)}")

    def execute(self):
        """执行完整备份流程"""
        try:
            backup_file = self.run_mysqldump()
            self.upload_to_oss(backup_file)
            self.clean_local_backups()
            # self.clean_oss_backups()
            logger.info("备份流程完成")
        except Exception as e:
            logger.critical(f"备份流程中断: {str(e)}")
            return False
        return True


if __name__ == "__main__":
    # 验证必要配置
    required_envs = [
        "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE",
        "OSS_ACCESS_KEY_ID", "OSS_ACCESS_KEY_SECRET",
        "OSS_ENDPOINT", "OSS_BUCKET"
    ]

    missing = [var for var in required_envs if not os.getenv(var)]
    if missing:
        logger.error(f"缺少必要环境变量: {', '.join(missing)}")
        exit(1)

    manager = BackupManager()
    success = manager.execute()
    exit(0 if success else 1)
