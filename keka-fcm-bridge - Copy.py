import json
import os
import requests
import paramiko
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import time

load_dotenv()


def fetch_access_token():
    url = os.getenv('KEKA_URL')
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    grant_type = os.getenv('GRANT_TYPE')
    scope = os.getenv('SCOPE')
    api_key = os.getenv('API_KEY')

    payload = (
        f"grant_type={grant_type}&"
        f"scope={scope}&"
        f"client_id={client_id}&"
        f"client_secret={client_secret}&"
        f"api_key={api_key}"
    )

    headers = {
        "accept": "application/json",
        "content-type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            if access_token:
                return access_token
            else:
                print("Access token not found in response.")
                return None
        else:
            print(
                f"Failed to retrieve token. Status code: {response.status_code}, Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print("Request failed:", e)
        return None


def call_second_api(access_token):
    second_api_url = "https://nephroplus.keka.com/api/v1/hris/employees"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }

    all_employees = []
    page = 1
    page_size = 200

    while True:
        params = {
            "page": page,
            "page_size": page_size
        }
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        emp_url = f"https://company.keka.com/api/v1/hris/employees?pageNumber={page}&pageSize=200"

        # response = requests.get(second_api_url, headers=headers, params=params)
        response = requests.get(emp_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            # print("================data", data)
            employees = data.get("data", [])
            total_pages = data.get("totalPages", 0)
            all_employees.extend(employees)

            # # Check if this is the last page
            print(
                f"page={page}, total pages={total_pages}, time={current_time}")
            if total_pages <= page:
                return all_employees
            # return all_employees

            page += 1
            time.sleep(1)  # Pause for 5 seconds
        else:
            print(
                f"Failed to fetch employee data. Status code: {response.status_code}, Response: {response.text}")
            break


def upload_to_ftp(employee_data):
    hostname = os.getenv('FTP_HOST_NAME')
    port = int(os.getenv('FTP_PORT'))
    username = os.getenv('FTP_USER_NAME')
    password = os.getenv('FTP_PASSWORD')
    data_to_write = []
    row_index = 0
    for employee in employee_data:
        # print("========row_index", row_index)
        if employee.get("employmentStatus") == 0 and employee.get("employeeNumber") not in {'TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'} and employee.get('bandInfo'):
            approver_employee_email = employee.get(
                'reportsTo', {}).get('email', '')
            approver_employee_info = next(
                (emp for emp in employee_data if emp.get(
                    'email') == approver_employee_email),
                None
            )

            group_title = next(
                (group['title'] for group in employee['groups'] if group['groupType'] == 3), None)

            l2Manager_email = employee.get(
                'l2Manager', {}).get('email', '')

            l2Manager_info = next(
                (emp for emp in employee_data if emp.get(
                    'email') == l2Manager_email),
                None
            )
            gender = None  # Default value
            prefix = None

            if employee.get('gender') == 1:
                gender = 'M'
            elif employee.get('gender') == 2:
                gender = 'F'

            if gender == "M":
                prefix = "Mr"
            elif gender == "F":
                prefix = "Ms"

            data_to_write.append([
                '',
                employee.get('email', ''),                     # Email
                employee.get('employeeNumber', ''),            # EmployeeID
                # Prefix (No info in given data)
                prefix,
                employee.get('firstName', ''),                  # FirstName
                employee.get('middleName', ''),                 # MiddleName
                employee.get('lastName', ''),                   # LastName
                # Suffix (No info in given data)
                '',
                # Gender (No info in given data)
                gender,
                employee.get('jobTitle', {}).get(
                    'title', ''),  # Title (Job Title)
                employee.get(
                    'reportsTo', {}).get('email', ''),
                approver_employee_info.get(
                    'employeeNumber', '') if approver_employee_info else 'NP001',  # ApproverEmail
                # employee.get('reportsTo', {}).get('email', ''),  # ApproverEmail
                # employee.get('reportsTo', {}).get(
                #     'id', ''),    # ApproverEmployeeID
                # Reporting1Data (No info in given data)
                employee.get('employeeNumber', ''),
                # Reporting2Data (No info in given data)
                employee.get('jobTitle', {}).get('title', ''),
                # Reporting3Data (No info in given data)
                'Ops',
                # Reporting4Data (No info in given data)
                group_title,
                # Reporting5Data (No info in given data)
                '',
                # Reporting6Data (No info in given data)
                employee.get('bandInfo', {}).get(
                    'title', 'NP Band') if employee.get('bandInfo') else None,
                # GroupIdentifier (No info in given data)
                '8A5FA38D-592E-4EE5-9DC2-1A984EFF6E68',
                # Email2Type (No info in given data)
                'P',
                # Email2 (No info in given data)
                employee.get('email', 'Test@nephroplus.com'),
                approver_employee_info.get(
                    'displayName', '') if approver_employee_info else 'Test ',  # ApproverEmail
                # employee.get('reportsTo', {}).get('email', ''),  # ApproverEmail
                # DefaultApprover1Email
                l2Manager_info.get(
                    'email', '') if l2Manager_info else approver_employee_info.get('email', '') if approver_employee_info else 'Test@nephroplus.com',
                # DefaultApprover1Name
                l2Manager_info.get('displayName', '') if l2Manager_info else approver_employee_info.get(
                    'displayName', '') if approver_employee_info else 'Test',
                # DefaultApprover1Name
                l2Manager_info.get(
                    'employeeNumber', '') if l2Manager_info else approver_employee_info.get('displayName', '') if approver_employee_info else 'NP12345',
                # employee.get('l2Manager', {}).get(
                #     'email', ''),     # DefaultApprover1EmployeeID
                employee.get('mobilePhone', ''),                 # CellPhone
                # OnlineEnabled (No info in given data)
                'TRUE'
            ])
        row_index += 1
    template_csv_path = os.getenv('TEMPLATE_FILE_PATH')

    df_template = pd.read_csv(template_csv_path)

    rows_needed = len(data_to_write)
    start_row = 0
    start_column = 0
    # if len(df_template) < start_row + rows_needed:
    #     additional_rows = start_row + rows_needed - len(df_template)
    #     df_template = pd.concat([df_template, pd.DataFrame(
    #         [[''] * df_template.shape[1]] * additional_rows)], ignore_index=True)

    if len(df_template) < start_row + rows_needed:
        additional_rows = start_row + rows_needed - len(df_template)

        # Ensure the DataFrame has exactly 27 columns
        if df_template.shape[1] < 27:
            # Create a list of new column names to match 27 columns
            required_columns = [f"Column_{i+1}" for i in range(27)]
            current_columns = list(df_template.columns)
            new_columns = [
                col for col in required_columns if col not in current_columns]

            for col in new_columns[:27 - df_template.shape[1]]:
                df_template[col] = ''
        elif df_template.shape[1] > 27:
            # Truncate extra columns
            df_template = df_template.iloc[:, :27]

        # Add additional rows
        df_template = pd.concat(
            [df_template, pd.DataFrame(
                [[''] * 27] * additional_rows, columns=df_template.columns)],
            ignore_index=True)

    # Insert the data into the template starting from the 4th row and 2nd column (index 1)
    for i, row_data in enumerate(data_to_write):
        row_index = start_row + i
        for j, value in enumerate(row_data):
            col_index = start_column + j
            df_template.iloc[row_index, col_index] = value

    # Save the modified DataFrame back to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = os.getenv('TARTGET_FILE_PATH')
    output_file_path = f"{output_file_path}/{timestamp}.csv"
    print("trying to save file in given path", output_file_path)
    df_template.to_csv(output_file_path, index=False)
    print("file saved at ", output_file_path)

    ftp_folder_pathe = os.getenv('FTP_FOLDER')
    remote_file_path = f"{ftp_folder_pathe}/{timestamp}.csv"

    print("trying to save file at FTP", remote_file_path)

    transport = paramiko.Transport((hostname, port))

    # try:
    #     transport.connect(username=username, password=password)
    #     sftp = paramiko.SFTPClient.from_transport(transport)
    #     sftp.put(output_file_path, remote_file_path)
    #     print(
    #         f"Successfully uploaded {output_file_path} to {remote_file_path}")

    # finally:
    #     # Close the SFTP session and transport
    #     sftp.close()
    #     transport.close()


def main():
    # # Load environment variables from .env file
    # load_dotenv()

    # # Fetch the access token
    access_token = fetch_access_token()

    if access_token:
        # Call the second API
        print("========token generated==============")
        api_response = call_second_api(access_token)
        if api_response:
            print("========employee data fetched ==============")
            upload_to_ftp(api_response)
    else:
        print("Failed to obtain access token.")


if __name__ == "__main__":
    main()
