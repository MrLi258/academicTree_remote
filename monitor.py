# monitor.py
import subprocess
import time
import logging
import os
from datetime import datetime

from config import fieldPart


# ================== 日志配置 ==================
def setup_logging(fieldPart):
    """配置日志记录系统"""
    # 创建日志目录
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 生成带时间戳的日志文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"monitor_{fieldPart}_{timestamp}.log")

    # 基础配置
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    # 单独配置子进程日志
    spider_logger = logging.getLogger("SPIDER")
    spider_logger.setLevel(logging.DEBUG)


# ================== 监控逻辑 ==================
def run_spider():
    # 设置系统代理
    env = os.environ.copy()
    env.update(
        {
            "HTTP_PROXY": "http://127.0.0.1:55992",
            "HTTPS_PROXY": "http://127.0.0.1:55992",
        }
    )

    """启动爬虫子进程"""
    return subprocess.Popen(
        [
            "D:\\python\\python_virtualenvs\\.PythonSpiderEnv\\Scripts\\python3.10.8.exe",
            "newTreeInfoGetApi.py",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        encoding="utf-8",  # 关键修复：强制指定编码
        errors="replace",  # 替换无法解码的字符
        universal_newlines=True,
    )


def handle_output(process):
    """实时处理子进程输出"""
    spider_logger = logging.getLogger("SPIDER")
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            spider_logger.info(line.strip())


def main():
    setup_logging(fieldPart)
    logger = logging.getLogger(__name__)

    MAX_RESTARTS = 100
    INITIAL_DELAY = 30
    current_delay = INITIAL_DELAY
    restarts = 0

    logger.info("====== 监控程序启动 ======")

    while restarts < MAX_RESTARTS:
        logger.info(f"尝试启动爬虫 (第 {restarts + 1}/{MAX_RESTARTS} 次)")

        try:
            process = run_spider()
            handle_output(process)  # 实时输出处理
            exit_code = process.wait()

            if exit_code == 0:
                logger.info("爬虫正常结束")
                return
            elif exit_code == 1:
                restarts += 1
                if restarts % 10 == 0:  # 每重启10次之后长休眠一段时间重新累计休眠时间
                    time.sleep(4000)
                    current_delay = INITIAL_DELAY
                logger.warning(f"检测到封禁 → {current_delay}秒后重启")
                time.sleep(current_delay)
                if current_delay < 1920:
                    current_delay *= 2  # 指数退避
            else:
                logger.error(f"异常退出码: {exit_code}")
                return

        except KeyboardInterrupt:
            logger.info("用户终止监控")
            return

    logger.error("达到最大重启次数，终止监控")


if __name__ == "__main__":
    main()
