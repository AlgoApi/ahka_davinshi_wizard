import logging
import os
import requests
import sys
from config import CHAT_ID_ADMIN, BOT_TOKEN
from datetime import datetime
from constc import ERROR_SIGNAL_DIR, LOG_FILE

def signal_error_folder() -> str:
    """
    Создаёт папку для сигнализации об ошибках с подпапкой по текущему времени.
    Возвращает путь к созданной подпапке.
    """
    os.makedirs(ERROR_SIGNAL_DIR, exist_ok=True)
    subdir = datetime.now().strftime('%Y%m%d_%H%M%S')
    full_path = os.path.join(ERROR_SIGNAL_DIR, subdir)
    os.makedirs(full_path, exist_ok=True)
    return full_path

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID_ADMIN, "text": text}
    try:
        response = requests.post(url, data=data)
        if response.status_code != 200:
            folder = signal_error_folder()
            with open(os.path.join(folder, 'status_error.txt'), 'w', encoding='utf-8') as f:
                f.write(f"HTTP {response.status_code}\n{text}")
            print(f"[Telegram send error {response.status_code}] folder: {folder}", file=sys.stderr)
    except Exception as e:
        folder = signal_error_folder()
        with open(os.path.join(folder, 'exception.txt'), 'w', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()}\n{e}\n")
        print(f"[Telegram send exception] {e}. folder: {folder}", file=sys.stderr)


class TelegramLogHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        if record.levelno >= logging.WARNING:
            try:
                send_telegram_message(log_entry)
            except Exception:
                folder = signal_error_folder()
                with open(os.path.join(folder, 'handler_emit_error.txt'), 'w', encoding='utf-8') as f:
                    f.write(f"{datetime.now().isoformat()}\nError in TelegramLogHandler.emit\n")
                self.handleError(record)

class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.msg = record.msg.encode('utf-8', 'replace').decode('utf-8')
        return super().format(record)

formatter = CustomFormatter(
    fmt='[%(asctime)s - %(levelname)s] - %(message)s',
    datefmt='%d-%m-%Y-%H:%M:%S'
)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)

logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

telegram_handler = TelegramLogHandler()
telegram_handler.setFormatter(formatter)
logger.addHandler(telegram_handler)