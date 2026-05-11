import logging
import logging.handlers
import re
import sys
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

class _PrintToLogger:
    def __init__(self, logger, level):
        self._logger = logger
        self._level = level
        self._buf = ""
        self._original = sys.__stdout__  # 원본 stdout 보관

    def write(self, msg):
        self._buf += msg
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                self._logger.log(self._level, line)

    def flush(self):
        if self._buf.strip():
            self._logger.log(self._level, self._buf)
            self._buf = ""

    # uvicorn/터미널이 요구하는 속성들
    def isatty(self): return False
    def fileno(self): return self._original.fileno()
    @property
    def encoding(self): return self._original.encoding
    @property
    def errors(self): return self._original.errors

def setup_logging(level: int = logging.INFO) -> None:
    log_format = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=date_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()

    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=LOG_DIR / "lapc.log",
        when="midnight",
        interval=1,
        backupCount=0,
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"

    def namer(default_name: str) -> str:
        base, ext, date = re.split(r"(\.log\.)", default_name)
        return f"{base}.{date}.log"

    file_handler.namer = namer
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler(sys.__stdout__)  # 원본 stdout 사용
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def redirect_print():
    """uvicorn.run() 호출 직전에 별도로 실행"""
    _print_logger = logging.getLogger("print")
    sys.stdout = _PrintToLogger(_print_logger, logging.INFO)