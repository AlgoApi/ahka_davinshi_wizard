from google.oauth2.service_account import Credentials
import gspread

# ---------------------------
# Подключение к Google Sheets (только для чтения)
# ---------------------------
# Задаём область доступа
scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
# Путь к файлу сервисного аккаунта (скачанный JSON с Google Cloud Console)
creds = Credentials.from_service_account_file('affable-ace-453114-d7-2c7d4db15b8a.json', scopes=scope)
gs_client = gspread.authorize(creds)