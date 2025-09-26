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
            # filtered_employees = [
            #     emp for emp in employees
            #     # if any(group.get("title") == "Support Office" and group.get("groupType") == 3 for group in emp.get("groups", []))
            #     if any(group.get("title") == "Support Office" or group.get("title") == "Support Zones" for group in emp.get("groups", []))
            #     and "gmail.com" not in (emp.get("email") or "")
            # ]
            all_employees.extend(employees)
            # all_employees.extend(filtered_employees)
            # print(employees)
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


def extract_band_value(band_info):
    # Split the band_info string by space and return the second part (the value after "band")
    parts = band_info.split()
    if len(parts) > 1:
        return parts[1]  # Return the second part (value after "band")
    return None  # Return None if the string doesn't contain a valid value after "band"


def upload_to_ftp(all_employees):
    hostname = os.getenv('FTP_HOST_NAME')
    port = int(os.getenv('FTP_PORT'))
    username = os.getenv('FTP_USER_NAME')
    password = os.getenv('FTP_PASSWORD')
    data_to_write = []
    data_to_write_dice = []
    row_index = 0
    npids = ["NP35593", "NP35564", "NP35556", "NP35549", "NP35532"]
    # employee_data = [
    #     record for record in employee_data
    #     if record.get('employeeNumber') in npids
    # ]

    all_employees = sorted(
        all_employees, key=lambda x: x.get("employeeNumber", ""))

    employee_data = all_employees

    employee_data = [
        record for record in employee_data
        if record.get('email') and "nephroplus.com" in record['email'].lower()
        if any(group.get("title") == "Support Office" or group.get("title") == "Support Zones" for group in record.get("groups", []))

        # and record.get("employeeNumber") == "NP33411"
    ]

    print("==================employee_data", len(employee_data))

    for employee in employee_data:
        # print("========row_index", row_index)
        # and employee.get('bandInfo'):
        if employee.get("employmentStatus") == 0 and employee.get("employeeNumber") not in {'TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'} and employee.get('bandInfo'):
            approver_employee_email = employee.get(
                'reportsTo', {}).get('email', '')
            approver_employee_info = next(
                (emp for emp in all_employees if emp.get(
                    'email') == approver_employee_email),
                None
            )

            group_title = next(
                (group['title'] for group in employee['groups'] if group['groupType'] == 3), None)

            l2Manager_email = employee.get(
                'l2Manager', {}).get('email', '')

            l2Manager_info = next(
                (emp for emp in all_employees if emp.get(
                    'email') == l2Manager_email),
                None
            )
            gender = None  # Default value
            prefix = None
            band_value = None
            if employee.get('bandInfo'):
                band_info = employee.get('bandInfo', {}).get(
                    'title', 'NP Band')  # Default value is 'NP Band'
                band_value = extract_band_value(band_info)

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
                    'employeeNumber', '') if approver_employee_info else '',  # ApproverEmail
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
                # employee.get('bandInfo', {}).get(
                #     'title', 'NP Band') if employee.get('bandInfo') else None,
                band_value,
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
                    'employeeNumber', '') if l2Manager_info else approver_employee_info.get('displayName', '') if approver_employee_info else '',
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
    folder_path = output_file_path
    output_file_path = f"{folder_path}/{timestamp}.csv"
    print("trying to save file in given path", output_file_path)
    df_template.to_csv(output_file_path, index=False)
    print("file saved at ", output_file_path)

    ftp_folder_pathe = os.getenv('FTP_FOLDER')
    remote_file_path = f"{ftp_folder_pathe}/{timestamp}.csv"

    # print("trying to save file at FTP", remote_file_path)

    # transport = paramiko.Transport((hostname, port))

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


# def diceConnection():
#     hostname_dice = os.getenv('DICE_FTP_HOST_NAME')
#     port_dice = int(os.getenv('FTP_PORT'))
#     username_dice = os.getenv('DICE_FTP_USER_NAME')
#     key_path = os.getenv('PEM_PATH')

#     key = paramiko.RSAKey.from_private_key_file(key_path)

#     transport = paramiko.Transport((hostname_dice, port_dice))
#     transport.connect(username=username_dice, pkey=key)

#     # Create SFTP client
#     sftp = paramiko.SFTPClient.from_transport(transport)

#     # Connect using private key
#     # try:
#     #     sftp.put(local_file_path, remote_file_path)
#     #     print(f"File uploaded to {remote_file_path}")
#     # except Exception as e:
#     #     print("Upload failed:", e)
#     # finally:
#     #     sftp.close()
#     #     transport.close()


def upload_to_ftp_dice(all_employees):
    hostname_dice = os.getenv('DICE_FTP_HOST_NAME')
    port_dice = int(os.getenv('FTP_PORT'))
    username_dice = os.getenv('DICE_FTP_USER_NAME')
    key_path = os.getenv('PEM_PATH')
    key = paramiko.RSAKey.from_private_key_file(key_path)

    data_to_write = []
    data_to_write_dice = []
    row_index = 0
    npids = ["NP35593", "NP35564", "NP35556", "NP35549", "NP35532"]
    # employee_data = [
    #     record for record in employee_data
    #     if record.get('employeeNumber') in npids
    # ]
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    all_employees = sorted(
        all_employees, key=lambda x: x.get("employeeNumber", ""))

    employee_data = all_employees

    # approvalEmails = []
    # cluster_managers = []

    # for employee in center_managers_data:
    #     reportsTo = employee.get('reportsTo', {}).get('email', '')

    #     if reportsTo:
    #         reportsToInfo = next(
    #             (emp for emp in all_employees if emp.get('email') == reportsTo),
    #             None
    #         )
    #         if reportsToInfo:
    #             emp_no = reportsToInfo.get('employeeNumber', '')
    #             if emp_no and emp_no not in cluster_managers:
    #                 if emp_no:
    #                     cluster_managers.append(emp_no)

    cluster_managers = [
        "NP16708", "NP30359", "NP30449", "NP35012", "NP32772", "NP27746", "NP29269",
        "NP33260", "NP33261", "NP33262", "NP33263", "NP33264", "NP33265", "NP33266",
        "NP33267", "NP33268", "NP33269", "NP33270", "NP33271", "NP33272", "NP6205",
        "NP29919", "NP31880", "NP26808", "NP29850", "NP34863", "NP33627", "NP32617",
        "NP35221", "NP35366", "NP32309", "NP33285", "NP30636", "NP32877", "NP29149",
        "NP29244", "NP32097", "NP31000", "NP11750", "NP11865", "NP10346", "NP16709",
        "NP11866", "NP30013", "Np28593", "NP34924", "NP31399", "NP29317", "NP31895",
        "NP33258", "NP34891"
    ]
    cluster_managers_lower = {emp.lower() for emp in cluster_managers}

    employee_data = [
        record for record in all_employees
        # if (record.get("secondaryJobTitle") or "").lower() in {"center manager", "cluster manager"}
        if ((record.get("secondaryJobTitle") or "").lower() in {"center manager", "cluster manager"}
            or (record.get("employeeNumber") or "").lower() in cluster_managers_lower)
    ]

    # all_employees = [
    #     record for record in all_employees
    #     if record.get('email') and any(domain in record['email'].lower() for domain in ["nephroplus.com", "nephroplus.in"])
    #     # if record.get("secondaryJobTitle") == "NP32538"
    # ]

    row_index = 0
    for employee in employee_data:
        # print("========row_index", row_index)
        # and employee.get('bandInfo'):
        employmentStatus = employee.get("employmentStatus")
        # if employee.get("employmentStatus") == 0 and employee.get("employeeNumber") not in {'TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'}:
        if employee.get("employeeNumber") not in {'TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'}:
            approver_employee_email = employee.get(
                'reportsTo', {}).get('email', '')
            secondaryJobTitle = employee.get("secondaryJobTitle", "")
            approver_employee_info = next(
                (emp for emp in all_employees if emp.get(
                    'email') == approver_employee_email),
                None
            )

            zone_info = next(
                (field['value'] for field in employee["customFields"] if 'zone' in field['title'].lower()), None)

            group_title = next(
                (group['title'] for group in employee['groups'] if group['groupType'] == 3), None)

            l2Manager_email = employee.get(
                'l2Manager', {}).get('email', '')

            l2Manager_info = next(
                (emp for emp in all_employees if emp.get(
                    'email') == l2Manager_email),
                None
            )
            gender = None  # Default value
            prefix = None
            band_value = None
            if employee.get('bandInfo'):
                band_info = employee.get('bandInfo', {}).get(
                    'title', 'NP Band')  # Default value is 'NP Band'
                band_value = extract_band_value(band_info)

            if employee.get('gender') == 1:
                gender = 'M'
            elif employee.get('gender') == 2:
                gender = 'F'

            if gender == "M":
                prefix = "Mr"
            elif gender == "F":
                prefix = "Ms"

            data_to_write_dice.append([
                employee.get('employeeNumber', ''),            # EmployeeID
                employee.get('firstName', ''),                  # FirstName
                employee.get('middleName', ''),                 # MiddleName
                employee.get('lastName', ''),                   # LastName
                gender,
                True if employmentStatus == 0 else False,
                employee.get('mobilePhone', ''),
                zone_info,  # zone
                group_title,  # Center/Location
                employee.get('email', ''),
                employee.get('jobTitle', {}).get('title', ''),   # Designation
                secondaryJobTitle,
                employee.get('reportsTo', {}).get('email', ''),
                approver_employee_info.get(
                    'employeeNumber', '') if approver_employee_info else '',  # ApproverEmail


                l2Manager_info.get('email', '') if l2Manager_info else approver_employee_info.get(
                    'email', '') if approver_employee_info else '',
                # DefaultApprover1Name
                l2Manager_info.get(
                    'employeeNumber', '') if l2Manager_info else approver_employee_info.get('employeeNumber', '') if approver_employee_info else '',
                # employee.get('l2Manager', {}).get(
                #     'email', ''),     # DefaultApprover1EmployeeID
            ])
        row_index += 1

    template_csv_path_dice = os.getenv('TEMPLATE_FILE_PATH_DICE')

    df_template_dice = pd.read_csv(template_csv_path_dice)

    rows_needed_dice = len(data_to_write_dice)
    start_row_dice = 0
    start_column_dice = 0

    dice_columns_count = 16
    if len(df_template_dice) < start_row_dice + rows_needed_dice:
        additional_rows = start_row_dice + \
            rows_needed_dice - len(df_template_dice)

        # Ensure the DataFrame has exactly 27 columns
        if df_template_dice.shape[1] < dice_columns_count:
            # Create a list of new column names to match 27 columns
            required_columns = [
                f"Column_{i+1}" for i in range(dice_columns_count)]
            current_columns = list(df_template_dice.columns)
            new_columns = [
                col for col in required_columns if col not in current_columns]

            for col in new_columns[:dice_columns_count - df_template_dice.shape[1]]:
                df_template_dice[col] = ''
        elif df_template_dice.shape[1] > dice_columns_count:
            # Truncate extra columns
            df_template_dice = df_template_dice.iloc[:, :dice_columns_count]

        # Add additional rows
        df_template_dice = pd.concat(
            [df_template_dice, pd.DataFrame(
                [[''] * dice_columns_count] * additional_rows, columns=df_template_dice.columns)],
            ignore_index=True)

    # Insert the data into the template starting from the 4th row and 2nd column (index 1)
    for i, row_data in enumerate(data_to_write_dice):
        row_index = start_row_dice + i
        for j, value in enumerate(row_data):
            col_index_dice = start_column_dice + j
            df_template_dice.iloc[row_index, col_index_dice] = value

    # Save the modified DataFrame back to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = os.getenv('TARTGET_FILE_PATH')
    folder_path = output_file_path
    output_file_path_dice = f"{folder_path}/Dice_{timestamp}.csv"
    df_template_dice.to_csv(output_file_path_dice, index=False)
    print("Dice file saved at ", output_file_path_dice)

    ftp_folder_pathe_dice = os.getenv('FTP_FOLDER_DICE')
    remote_file_path_dice = f"{ftp_folder_pathe_dice}/Dice_{timestamp}.csv"

    print("trying to save file at FTP DICE", remote_file_path_dice)

    # transport_dice = paramiko.Transport((hostname_dice, port_dice))

    # try:
    #     transport_dice.connect(username=username_dice, pkey=key)
    #     sftp = paramiko.SFTPClient.from_transport(transport_dice)
    #     sftp.put(output_file_path_dice, remote_file_path_dice)
    #     print(
    #         f"Successfully uploaded {output_file_path} to {remote_file_path_dice}")

    # finally:
    #     # Close the SFTP session and transport
    #     sftp.close()
    #     transport_dice.close()


def main():
    # # Load environment variables from .env file
    # load_dotenv()
    # # Fetch the access token
    access_token = fetch_access_token()

    if access_token:
        # Call the second API
        print("========token generated==============")
        api_response = call_second_api(access_token)
        print("==================api_response", len(api_response))
        if api_response:
            print("========employee data fetched ==============")
            upload_to_ftp(api_response)
            # upload_to_ftp_dice(api_response)
    else:
        print("Failed to obtain access token.")


if __name__ == "__main__":
    main()
