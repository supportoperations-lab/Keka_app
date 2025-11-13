import os
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ======================================
# CONFIGURATION
# ======================================
KEKA_DOMAIN = "https://nephroplus.keka.com/api/v1/hris/employees"
EMPLOYEE_API = f"{KEKA_DOMAIN}/api/v1/hris/employees"
ATTENDANCE_API = f"{KEKA_DOMAIN}/api/v1/time/attendance"

CLIENT_ID = "YOUR_KEKA_CLIENT_ID"
CLIENT_SECRET = "YOUR_KEKA_CLIENT_SECRET"

SERVICE_ACCOUNT_FILE = "service-account.json"  # your service account JSON key
DRIVE_FOLDER_ID = "YOUR_GOOGLE_DRIVE_FOLDER_ID"

# ======================================
# FUNCTIONS
# ======================================

def get_keka_token():
    """Get new Keka access token"""
    url = f"{KEKA_DOMAIN}/connect/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    token = response.json()["access_token"]
    print("✅ Keka access token generated")
    return token


def get_active_employees(token):
    """Fetch active employees list"""
    employees = []
    headers = {"Authorization": f"Bearer {token}"}
    page = 1
    while True:
        print(f"Fetching employee page {page}...")
        response = requests.get(f"{EMPLOYEE_API}?pageNumber={page}&pageSize=100", headers=headers)
        if response.status_code != 200:
            print("❌ Failed to fetch employee list:", response.text)
            break

        data = response.json()
        employee_data = data.get("data", [])
        if not employee_data:
            break

        # Keep only active employees (avoid test or inactive)
        filtered = [
            e for e in employee_data
            if e.get("employmentStatusName") == "Active"
            and not str(e.get("employeeNumber", "")).startswith("TEST")
        ]
        employees.extend(filtered)

        if not data.get("hasMore", False):
            break
        page += 1
        time.sleep(1)
    print(f"✅ Total active employees fetched: {len(employees)}")
    return employees


def fetch_yesterday_attendance(token, employees):
    """Fetch attendance for yesterday (single date)"""
    headers = {"Authorization": f"Bearer {token}"}
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    all_records = []
    missing_records = []

    print(f"📅 Fetching attendance for: {yesterday}")

    for idx, emp in enumerate(employees, start=1):
        emp_id = emp["employeeId"]
        emp_num = emp["employeeNumber"]
        url = f"{ATTENDANCE_API}?employeeIds={emp_id}&startDate={yesterday}&endDate={yesterday}"

        response = requests.get(url, headers=headers)

        # Handle token expiry
        if response.status_code == 401:
            print("⚠️ Token expired — refreshing...")
            token = get_keka_token()
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"❌ Failed to fetch attendance for {emp_num}")
            continue

        data = response.json().get("data", [])
        if not data:
            missing_records.append(emp_num)
            continue

        record = data[0]
        first_in = record.get("firstInOfTheDay")
        last_out = record.get("lastOutOfTheDay")

        # Handle missing punches
        if not first_in or not last_out:
            status = record.get("status", "")
            if status.lower() != "approved":
                print(f"⚠️ Skipping pending/unapproved attendance for {emp_num}")
                continue
            missing_records.append(emp_num)

        all_records.append({
            "EmployeeNumber": emp_num,
            "EmployeeName": emp.get("displayName"),
            "CenterCode": emp.get("department", {}).get("name", ""),
            "AttendanceDate": yesterday,
            "FirstIn": first_in.get("timestamp") if first_in else None,
            "LastOut": last_out.get("timestamp") if last_out else None,
            "Status": record.get("status"),
        })

        # Prevent throttling
        time.sleep(1.2)

        if idx % 100 == 0:
            print(f"Processed {idx}/{len(employees)} employees...")

    print(f"✅ Attendance fetched for {len(all_records)} employees")
    print(f"⚠️ Missing attendance for {len(missing_records)} employees")

    return all_records, missing_records


def upload_to_drive(file_path, file_name):
    """Upload file to Google Drive"""
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    service = build("drive", "v3", credentials=creds)

    file_metadata = {"name": file_name, "parents": [DRIVE_FOLDER_ID]}
    media = MediaFileUpload(file_path, mimetype="text/csv")

    service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"✅ Uploaded to Google Drive: {file_name}")


# ======================================
# MAIN EXECUTION
# ======================================
def main():
    try:
        token = get_keka_token()
        employees = get_active_employees(token)

        all_records, missing = fetch_yesterday_attendance(token, employees)

        # Save to CSV
        df = pd.DataFrame(all_records)
        file_name = f"keka_attendance_{datetime.today().strftime('%Y%m%d')}.csv"
        file_path = os.path.join(os.getcwd(), file_name)
        df.to_csv(file_path, index=False)
        print(f"📁 Attendance saved locally: {file_path}")

        # Upload to Drive
        upload_to_drive(file_path, file_name)

        # Log missing employees
        if missing:
            pd.DataFrame({"MissingEmployeeNumber": missing}).to_csv(
                "missing_attendance.csv", index=False
            )
            print(f"⚠️ Missing data logged: missing_attendance.csv")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
