from pathlib import Path

DATABASE_DSN = "postgresql://login:pass@ip:port/db_name"
# Настройки каталогов
REPO_SESSION_DIR = Path(r'C:\Users\Administrator\Documents\SessionRepository')
REPO_SESSIONCHECKER_DIR = Path(r'C:\Users\Administrator\Documents\SessionCheckerRepository')
DEST_SESSION_DIR = Path(r'C:\Users\Administrator\Documents\tgbot\autolike\sessions')
DEST_SESSIONCHECKER_DIR = Path(r'C:\Users\Administrator\Documents\tgbot\autolike\counter\sessions_checker')
# убрать ниже и раскоментить выше!
#REPO_SESSION_DIR = Path(r'/home/algoapi/Рабочий стол/GIGATEST/CONCURENT/autolike/SessionRepository')
#REPO_SESSIONCHECKER_DIR = Path(r'/home/algoapi/Рабочий стол/GIGATEST/CONCURENT/autolike/SessionCheckerRepository')
#DEST_SESSION_DIR = Path(r'/home/algoapi/Рабочий стол/GIGATEST/CONCURENT/autolike/sessions')
#DEST_SESSIONCHECKER_DIR = Path(r'/home/algoapi/Рабочий стол/GIGATEST/CONCURENT/autolike/sessions_checker')

SESSIONS_DIR = './sessions_checker/'
LOG_FILE = './log/checker.log'
ERROR_SIGNAL_DIR = './error_signals'
# Фиксированные расширения для операций
FILE_EXTENSIONS = ['.session', '.json']
