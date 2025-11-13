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
TARGET_FILE_PATH = os.getenv("TARGET_FILE_PATH", "output")
FTP_FOLDER = os.getenv("FTP_FOLDER", "Nephrocare")
FTP_FOLDER_DICE = os.getenv("FTP_FOLDER_DICE", "nephroplus_hrms")

# === Google Drive variables ===
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "gcp_key.json")

# Ensure output folder exists
os.makedirs(TARGET_FILE_PATH, exist_ok=True)


# === Step 1: Fetch Access Token ===
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
            print(f"❌ Token fetch failed ({response.status_code}): {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print("❌ Request failed:", e)
        return None


# === Step 2: Fetch Employee Data ===
def call_second_api(access_token):
    all_employees = []
    page = 1
    page_size = 200
    base_url = "https://nephroplus.keka.com/api/v1/hris/employees"

    while True:
        emp_url = f"{base_url}?pageNumber={page}&pageSize={page_size}"
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

        response = requests.get(emp_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            employees = data.get("data", [])
            total_pages = data.get("totalPages", 0)
            all_employees.extend(employees)
            print(f"Page {page}/{total_pages} fetched ({len(employees)} employees)")
            if total_pages <= page:
                break
            page += 1
            time.sleep(2)
        elif response.status_code == 401:
            print("⚠️ Token expired while fetching employees, refreshing...")
            access_token = fetch_access_token(os.getenv('API_KEY'))
        else:
            print(f"❌ Failed to fetch employees: {response.text}")
            break

    return all_employees


# === Step 3: Helper - Convert Timestamps ===
def convert_timestamp(timestamp):
    if timestamp and isinstance(timestamp, str):
        try:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return ""
    return ""


# === Step 4: Upload File to Google Drive ===
def upload_to_drive(file_path, file_name):
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
            supportsAllDrives=True
        ).execute()

        print(f"✅ File uploaded to Drive (ID: {uploaded_file.get('id')})")

    except Exception as e:
        print(f"❌ Google Drive upload failed: {e}")


# === Step 5: Fetch Attendance with Retry Logic ===
def fetch_attendance_for_employee(emp_id, headers, start_date, end_date, retries=3):
    url = f"https://nephroplus.keka.com/api/v1/time/attendance?employeeIds={emp_id}&from={start_date}&to={end_date}"
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json().get("data", [])
                if data:
                    return data
            elif response.status_code == 401:
                print("⚠️ Token expired during attendance fetch, refreshing...")
                headers["Authorization"] = f"Bearer {fetch_access_token(os.getenv('API_KEY_ATTENDANCE'))}"
            elif response.status_code == 429:
                print("⏳ Rate limit hit, retrying...")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"⚠️ Unexpected response {response.status_code}: {response.text}")
        except Exception as e:
            print(f"❌ Error fetching attendance (attempt {attempt + 1}): {e}")
        time.sleep(2)
    return []


# === Step 6: Get All Attendance Data ===
def get_employee_attendance(employee_data, access_token, start_date=None, end_date=None):
    if not start_date or not end_date:
        # Fetch for yesterday and today to cover late punches
        start_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = (datetime.today()).strftime("%Y-%m-%d")

    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    data_to_write = []
    missing_entries = []

    employee_data = sorted(
        [
            e for e in employee_data
            if e.get("employmentStatus") == 0 and
            e.get("employeeNumber") not in {"TEST001", "TEST002", "TEST003", "TEST004", "TEST005"}
        ],
        key=lambda e: e.get("employeeNumber", "")
    )

    for idx, emp in enumerate(employee_data, start=1):
        emp_id = emp.get("id")
        employeeNumber = emp.get("employeeNumber")

        attendance_records = fetch_attendance_for_employee(emp_id, headers, start_date, end_date)

        for att in attendance_records:
            first_in = att.get("firstInOfTheDay")
            last_out = att.get("lastOutOfTheDay")

            # Detect missing data
            if not first_in or not last_out:
                missing_entries.append({
                    "employeeNumber": employeeNumber,
                    "date": att.get("attendanceDate"),
                    "status": att.get("status")
                })

            employee_info = next((rec for rec in employee_data if rec.get("employeeNumber") == employeeNumber), None)
            group_title = None
            if employee_info:
                groups = employee_info.get('groups', [])
                group_title = next((g['title'] for g in groups if g.get('groupType') == 3), None)

            data_to_write.append([
                att.get("id"),
                employeeNumber,
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

        if idx % 50 == 0:
            print(f"Processed {idx} employees...")

    # === Save to CSV ===
    df_template = pd.read_csv(ATT_TEMPLATE_FILE_PATH)
    rows_needed = len(data_to_write)
    columns_count = 17

    if len(df_template) < rows_needed:
        additional_rows = rows_needed - len(df_template)
        df_template = pd.concat([df_template, pd.DataFrame([[''] * columns_count] * additional_rows, columns=df_template.columns)], ignore_index=True)

    for i, row_data in enumerate(data_to_write):
        for j, value in enumerate(row_data):
            df_template.iloc[i, j] = value

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_name = f"attendance_{start_date}_{end_date}_{timestamp}.csv"
    output_file_path = os.path.join(TARGET_FILE_PATH, output_file_name)
    df_template.to_csv(output_file_path, index=False)
    print(f"✅ Attendance file saved: {output_file_path}")

    # === Log Missing Entries ===
    if missing_entries:
        missing_df = pd.DataFrame(missing_entries)
        missing_file = os.path.join(TARGET_FILE_PATH, f"missing_attendance_{timestamp}.csv")
        missing_df.to_csv(missing_file, index=False)
        print(f"⚠️ Missing in/out entries logged: {missing_file}")

    # Upload final CSV to Drive
    upload_to_drive(output_file_path, output_file_name)


# === Step 7: Main Entry Point ===
def main():
    api_key = os.getenv('API_KEY')
    api_key_attendance = os.getenv('API_KEY_ATTENDANCE')

    list_access_token = fetch_access_token(api_key)
    att_access_token = fetch_access_token(api_key_attendance)

    if list_access_token and att_access_token:
        employee_data = call_second_api(list_access_token)
        print(f"✅ Total active employees fetched: {len(employee_data)}")
        get_employee_attendance(employee_data, att_access_token)
    else:
        print("❌ Failed to obtain access tokens.")


if __name__ == "__main__":
    main()
