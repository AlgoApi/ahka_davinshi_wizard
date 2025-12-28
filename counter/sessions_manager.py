import json
import shutil
from constc import *
from pathlib import Path
from google_service import *
from misc_func import *

agent_mapping = {}


def scope_sheet_data():
    global agent_mapping
    agent_mapping = {}
    # Открываем таблицу по её ID (ключу)
    sheet_id = "1Vy-Q7Ro3H7oSynUSCaC7rWXlNXeowbqakofvc63ICwo"  # замените на ID вашей таблицы
    sheet = gs_client.open_by_key(sheet_id)
    worksheet = sheet.get_worksheet(0)  # читаем первый лист

    # Получаем все записи из таблицы как список словарей
    records = worksheet.get_all_records()

    for record in records:
        phone = normalize_phone(str(record.get("Number")).strip())  # например, "+1234567890"
        agent_name = str(record.get("Username")).strip()  # например, "AgentName"
        status = True if str(record.get("Active", "Нет")).strip() == "Да" else False
        only_checker = True if str(record.get("only_checker", "Нет")).strip() == "Да" else False
        only_ashqua = True if str(record.get("only_ashqua", "Нет")).strip() == "Да" else False
        agent_mapping[phone] = {"name" : agent_name,
                                "active" : status,
                                "only_checker": only_checker,
                                "only_ashqua": only_ashqua}
        del only_ashqua, only_checker, status, agent_name, phone
    return agent_mapping

def move_files(base_names: set[str], code: int):
    """
    Перемещение файлов с заданными базовыми именами из указанного каталога обратно в репозиторий.
    Операции проводятся для расширений .session и .json.
    """

    # Валидация входных данных
    if not isinstance(base_names, set) or not all(isinstance(n, str) for n in base_names):
        return {'error': 'Parameter "names" must be a list of strings'}
    if not isinstance(code, int):
        return {'error': 'Parameter "code" must be a int'}

    src_dir = DEST_SESSION_DIR if code == 1 else DEST_SESSIONCHECKER_DIR
    repo_dir = REPO_SESSION_DIR if code == 1 else REPO_SESSIONCHECKER_DIR

    if not isinstance(src_dir, Path):
        return {'error': 'Parameter "src_dir" must be a Path'}
    if not isinstance(repo_dir, Path):
        return {'error': 'Parameter "repo_dir" must be a Path'}

    result = {'moved': [], 'alarm': [], 'errors': {}}

    for name in base_names:
        moved_pair = []
        has_error = False
        done = False
        for ext in FILE_EXTENSIONS:
            src_path = src_dir.joinpath(f"{name}{ext}")
            dest_path = repo_dir.joinpath(f"{name}{ext}")
            if src_path.exists():
                try:
                    if dest_path.exists():
                        dest_path.unlink()
                    shutil.move(str(src_path), str(dest_path))
                    src_path_text = '\\'.join(str(src_path).split('\\')[-2:])
                    dest_path_text = '\\'.join(str(dest_path).split('\\')[-2:])
                    moved_pair.append(f"from: {src_path_text}, \nto: {dest_path_text} \n")
                    done = True
                except Exception as e:
                    has_error = True
                    result['errors'][f"{name}{ext}"] = str(e)
            else:
                result['errors'][f"{name}{ext}"] = "src 404"
        if has_error:
            result['alarm'].append("{name}{ext}")
        elif done:
            result['moved'].append(moved_pair)

    return result


def add_files(base_names: set[str], code: int):

    # Валидация входных данных
    if not isinstance(base_names, set) or not all(isinstance(n, str) for n in base_names):
        return {'error': 'Parameter "names" must be a list of strings'}
    if not isinstance(code, int):
        return {'error': 'Parameter "code" must be a int'}

    dest_dir = DEST_SESSION_DIR if code == 1 else DEST_SESSIONCHECKER_DIR
    repo_dir = REPO_SESSION_DIR if code == 1 else REPO_SESSIONCHECKER_DIR
    if not isinstance(dest_dir, Path):
        return {'error': 'Parameter "dest_dir" must be a Path'}
    if not isinstance(repo_dir, Path):
        return {'error': 'Parameter "repo_dir" must be a Path'}
    result = {'added': [], 'errors': {}}

    for name in base_names:
        moved_pair = []
        done = False
        for ext in FILE_EXTENSIONS:
            src_path = repo_dir.joinpath(f"{name}{ext}")
            dest_path = dest_dir.joinpath(f"{name}{ext}")
            if src_path.exists():
                try:
                    if dest_path.exists():
                        result['errors'][f"{name}{ext}"] = "dest_path exists"
                        continue
                    only_checker = True if agent_mapping.get(name).get("only_checker") == "Да" else False
                    only_ashqua = True if agent_mapping.get(name).get("only_ashqua") == "Да" else False
                    if only_ashqua and code == 1:
                        result['errors'][f"{name}{ext}"] = f"prevent operation add to DEST_SESSION_DIR {only_ashqua}"
                        continue
                    if ext == '.json':
                        try:
                            with open(src_path, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            # Update flags in JSON
                            data['only_checker'] = only_checker
                            data['only_ashqua'] = only_ashqua
                            # Write back to the same file before moving
                            with open(src_path, 'w', encoding='utf-8') as f:
                                json.dump(data, f, ensure_ascii=False, indent=4)
                        except Exception as e:
                            result['errors'][f"{name}{ext}"] = f"json update failed: {e}"
                            continue
                    shutil.move(str(src_path), str(dest_path))
                    src_path_text = '\\'.join(str(src_path).split('\\')[-2:])
                    dest_path_text = '\\'.join(str(dest_path).split('\\')[-2:])
                    moved_pair.append(f"from: {src_path_text}, \nto: {dest_path_text} \n")
                    done = True
                except Exception as e:
                    result['errors'][f"{name}{ext}"] = str(e)
            else:
                result['errors'][f"{name}{ext}"] = "src 404"
        if done:
            result['added'].append(moved_pair)
    return result