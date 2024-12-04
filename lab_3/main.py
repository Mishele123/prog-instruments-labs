import csv
import re

from typing import List
from checksum import calculate_checksum, serialize_result

VAR = 62
CSV_FILE = "lab_3/62.csv"
JSON_FILE = "lab_3/result.json"
REGULARS = {
    "telephone": r"^\+7-\(\d{3}\)-\d{3}-\d{2}-\d{2}$",
    "http_status_message": r"^\d{3}\s[^\n\r]+$", 
    "inn": r"^\d{12}$",
    "identifier": r"^\d{2}-\d{2}/\d{2}$",              
    "ip_v4": r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|"
             r"[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$",
    "latitude": r"^[+-]?(90(\.0+)?|([0-8]?\d(\.\d+)?))$",
    "blood_type": r"^(?:AB|A|B|O)[+\u2212]$",             
    "isbn": r"^\d+-\d+-\d+-\d+(:?-\d+)?$", 
    "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",  
    "date": r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-9]|3[0-1])$"
}


def load_cvs_data(file_path: str) -> List[str]:
    """
    Чтение данных из cvs файла
    """
    with open(file_path, "r", encoding="utf-16") as file:
        reader = csv.reader(file, delimiter=";")
        next(reader)
        return [line for line in reader]
    

def check_valid_row(row: List[str]) -> bool:
    """
    Проверяет, удовлетворяет ли строка регулярным выражениям
    """
    for index, value in enumerate(row):
        regular = list(REGULARS.values())[index]
        if not re.match(regular, value):
            return False
    return True


def get_invalid_rows(rows: List[str]) -> List[str]:
    """
    Поиск невалидных строк
    """
    return [index for index, row in enumerate(rows) if not check_valid_row(row)]


if __name__ == "__main__":
    data = load_cvs_data(CSV_FILE)
    invalid_rows = get_invalid_rows(data)
    checksum_result = calculate_checksum(invalid_rows)
    serialize_result(VAR, checksum_result, JSON_FILE)
