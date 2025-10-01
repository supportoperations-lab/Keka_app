import json
import os
import requests
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pandas as pd
import time

# === File paths ===
TEMPLATE_FILE_PATH_DICE = os.getenv("TEMPLATE_FILE_PATH_DICE", "Dice_SFTP_Template.csv")
TEMPLATE_FILE_PATH = os.getenv("TEMPLATE_FILE_PATH", "SFTP_File-Nephrocare-27dec.csv")
ATT_TEMPLATE_FILE_PATH = os.getenv("ATT_TEMPLATE_FILE_PATH", "Attendance.csv")
TARGET_FILE_PATH = os.getenv("TARGET_FILE_PATH", "output")  # output folder inside repo
FTP_FOLDER = os.getenv("FTP_FOLDER", "Nephrocare")
FTP_FOLDER_DICE = os.getenv("FTP_FOLDER_DICE", "nephroplus_hrms")

# === Google Drive variables ===
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")  # Folder ID from Google Drive
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "gcp_key.json")  # Path to service account file

# Ensure output folder exists
os.makedirs(TARGET_FILE_PATH, exist_ok=True)


def fetch_access_token(api_key_attendance):
    url = os.getenv('KEKA_URL')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    grant_type = os.getenv('GRANT_TYPE')
    scope = os.getenv('SCOPE')

    payload = (
        f"grant_type={grant_type}&"
        f"scope={scope}&"
        f"client_id={client_id}&"
        f"client_secret={client_secret}&"
        f"api_key={api_key_attendance}"
    )

    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0",
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            token_data = response.json()
            return token_data.get("access_token")
        else:
            print(f"❌ Failed to retrieve token. Status code: {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print("❌ Request failed:", e)
        return None


def call_second_api(access_token):
    all_employees = []
    page = 1
    page_size = 200

    while True:
        emp_url = f"https://company.keka.com/api/v1/hris/employees?pageNumber={page}&pageSize={page_size}"
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

        response = requests.get(emp_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            employees = data.get("data", [])
            total_pages = data.get("totalPages", 0)
            all_employees.extend(employees)
            print(f"page={page}, total pages={total_pages}, time={datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            if total_pages <= page:
                return all_employees
            page += 1
            time.sleep(2)
        else:
            print(f"❌ Failed to fetch employee data. Status code: {response.status_code}, Response: {response.text}")
            break


def convert_timestamp(timestamp):
    if timestamp and isinstance(timestamp, str):
        try:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""
    return ""


def upload_to_drive(file_path, file_name):
    """Uploads the file to Google Drive (Shared Drive)."""
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)

        file_metadata = {"name": file_name}
        if GDRIVE_FOLDER_ID:
            file_metadata["parents"] = [GDRIVE_FOLDER_ID]

        media = MediaFileUpload(file_path, mimetype="text/csv")

        uploaded_file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True  # 🔹 Required for Shared Drives
        ).execute()

        print(f"✅ File uploaded to Shared Drive with ID: {uploaded_file.get('id')}")

    except Exception as e:
        print(f"❌ Google Drive upload failed: {e}")


def get_employee_attendance(employee_data, access_token, start_date=None, end_date=None):
    if not start_date or not end_date:
        yesterday =  (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_date = yesterday
        end_date = yesterday
    data_to_write = []

    # Filter employees
    employee_data = sorted(
        [
            employee for employee in employee_data
            if employee.get("employmentStatus") == 0
            and employee.get("employeeNumber") not in {"TEST001", "TEST002", "TEST003", "TEST004", "TEST005"}
        ],
        key=lambda e: e.get("employeeNumber", "")
    )

    employee_data = employee_data[:5]

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    employee_attendance_data = []

    for row_index, employee in enumerate(employee_data):
        emp_id = employee.get("id")
        emp_url = f"https://nephroplus.keka.com/api/v1/time/attendance?employeeIds={emp_id}&from={start_date}&to={end_date}"

        try:
            response = requests.get(emp_url, headers=headers)
            if response.status_code == 200:
                result = response.json()
                employee_attendance_data.extend(result.get("data", []))
            else:
                print(f"❌ Failed to fetch attendance for {employee.get('employeeNumber')}")
        except Exception as e:
            print(f"❌ Error fetching attendance: {e}")
        time.sleep(1.5)

    for att in employee_attendance_data:
        employeeNumber = att.get("employeeNumber", "")
        employee_info = next((rec for rec in employee_data if rec.get("employeeNumber") == employeeNumber), None)
        group_title = None
        if employee_info:
            groups = employee_info.get('groups', [])
            group_title = next((g['title'] for g in groups if g.get('groupType') == 3), None)

        first_name = employee_info.get("firstName", "") if employee_info else ""
        last_name = employee_info.get("lastName", "") if employee_info else ""
        
        first_in = att.get("firstInOfTheDay")
        last_out = att.get("lastOutOfTheDay")
        data_to_write.append([
            att.get("id"),
            employeeNumber,
            first_name,
            last_name,
            group_title,
            employee_info.get('jobTitle', {}).get('title', '') if employee_info else "",
            att.get("attendanceDate"),
            att.get("shiftStartTime"),
            att.get("shiftEndTime"),
            convert_timestamp(first_in.get("timestamp")) if isinstance(first_in, dict) else "",
            convert_timestamp(last_out.get("timestamp")) if isinstance(last_out, dict) else "",
            att.get("dayType"),
            att.get("shiftDuration"),
            att.get("shiftEffectiveDuration"),
            att.get("totalGrossHours"),
            att.get("totalEffectiveHours"),
            att.get("totalBreakDuration"),
            att.get("totalEffectiveOvertimeDuration"),
            att.get("totalGrossOvertimeDuration")
        ])

    df_template = pd.read_csv(ATT_TEMPLATE_FILE_PATH)
    rows_needed = len(data_to_write)
    columns_count = 19

    if len(df_template) < rows_needed:
        additional_rows = rows_needed - len(df_template)
        df_template = pd.concat([df_template, pd.DataFrame([['']*columns_count]*additional_rows, columns=df_template.columns)], ignore_index=True)

    for i, row_data in enumerate(data_to_write):
        for j, value in enumerate(row_data):
            df_template.iloc[i, j] = value

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_name = f"att_{start_date}_{end_date}_{timestamp}.csv"
    output_file_path = os.path.join(TARGET_FILE_PATH, output_file_name)
    df_template.to_csv(output_file_path, index=False)
    print(f"📂 Attendance file saved at: {output_file_path}")

    # Upload to Google Drive
    upload_to_drive(output_file_path, output_file_name)


def main():
    api_key = os.getenv('API_KEY')
    api_key_attendance = os.getenv('API_KEY_ATTENDANCE')

    list_access_token = fetch_access_token(api_key)
    att_access_token = fetch_access_token(api_key_attendance)

    if list_access_token and att_access_token:
        api_response = call_second_api(list_access_token)
        if api_response:
            print(f"✅ Fetched employee data: {len(api_response)}")
            get_employee_attendance(api_response, att_access_token)
    else:
        print("❌ Failed to obtain access tokens.")


if __name__ == "__main__":
    main()
