from fastapi.responses import StreamingResponse, FileResponse
import asyncio
from fastapi import FastAPI
import json
import os
import httpx
import paramiko
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles

app = FastAPI()
load_dotenv()


async def fetch_access_token():
    """ Fetch access token with yield-based streaming """
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
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            print("token", {access_token})
            # yield f"data: Access token fetched successfully\n\n"
            return access_token
            # yield access_token  # Yield the token instead of returning it
        else:
            return None
            # yield f"data: Failed to retrieve token. Status: {response.status_code}\n\n"
            # yield None
    except httpx.RequestError as e:
        # yield f"data: Request failed: {e}\n\n"
        print("data: Request failed", e)
        # yield None


async def call_second_api(access_token):
    """ Fetch employee data page by page """
    headers = {"Authorization": f"Bearer {access_token}",
               "Accept": "application/json"}
    all_employees = []
    page = 1

    async with httpx.AsyncClient() as client:
        while True:
            emp_url = f"https://company.keka.com/api/v1/hris/employees?pageNumber={page}&pageSize=200"

            try:
                response = await client.get(emp_url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    employees = data.get("data", [])
                    total_pages = data.get("totalPages", 0)

                    # Filter employees by group
                    # filtered_employees = [
                    #     emp for emp in employees
                    #     if any(group.get("title") in ["Support Office", "Support Zones"] for group in emp.get("groups", []))
                    # ]

                    # all_employees.extend(filtered_employees)
                    all_employees.extend(employees)
                    print(
                        f"page={page}, total pages={total_pages}")
                    # yield json.dumps({"message": f"Fetched page {page} of {total_pages}"})
                    if page == 1:
                        yield f"data: Total Pages {total_pages}\n\n"
                    if page % 5 == 0:
                        yield f"data: || page: {page:03d} ||\n"
                    else:
                        yield f"data: || page: {page:03d} || "

                    if page >= total_pages:
                        break

                    page += 1
                    await asyncio.sleep(1)
                else:
                    yield json.dumps({"error": f"Failed to fetch employee data. Status code: {response.status_code}"})
                    break

            except httpx.RequestError as e:
                yield json.dumps({"error": f"Request failed: {str(e)}"})
                break

    # Ensure final data is properly formatted
    yield json.dumps({"employees": all_employees})


def extract_band_value(band_info):
    # Split the band_info string by space and return the second part (the value after "band")
    parts = band_info.split()
    if len(parts) > 1:
        return parts[1]  # Return the second part (value after "band")
    return None  # Return None if the string doesn't contain a valid value after "band"


async def upload_to_ftp(all_employees):
    """ Upload data to FTP and stream progress """
    yield "\n\n"
    yield "data: Preparing data for FTP upload...\n\n"

    hostname = os.getenv('FTP_HOST_NAME')
    port = int(os.getenv('FTP_PORT'))
    username = os.getenv('FTP_USER_NAME')
    password = os.getenv('FTP_PASSWORD')
    data_to_write = []
    row_index = 0

    all_employees = sorted(
        all_employees, key=lambda x: x.get("employeeNumber", ""))

    employee_data = all_employees

    employee_data = [
        record for record in employee_data
        if record.get('email') and "nephroplus.com" in record['email'].lower()
        if any(group.get("title") in ["Support Office", "Support Zones"] for group in record.get("groups", []))

        # and record.get("employeeNumber") == "NP35549"
    ]
    yield f"data: Total employee_data {len(employee_data)}\n\n"
    for employee in employee_data:
        # print("========row_index", row_index)
        # if employee.get("employmentStatus") == 0 and employee.get("employeeNumber") not in {'TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005'} and employee.get('bandInfo'):
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

    yield "data: Generating to CSV \n\n"

    # Save the modified DataFrame back to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file_path = os.getenv('TARTGET_FILE_PATH')
    output_file_path = f"{output_file_path}/{timestamp}.csv"
    print("trying to save file in given path", output_file_path)
    df_template.to_csv(output_file_path, index=False)
    print("file saved at ", output_file_path)
    yield f"data: Saved filet at {output_file_path} \n\n"

    ftp_folder_pathe = os.getenv('FTP_FOLDER')
    remote_file_path = f"{ftp_folder_pathe}/{timestamp}.csv"

    print("trying to save file at FTP", remote_file_path)
    yield f"data: Trying to save file at SFTP  at {remote_file_path} \n\n"

    transport = paramiko.Transport((hostname, port))

    try:
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(output_file_path, remote_file_path)
        yield f"data: File successfully upload to SFTP  at {remote_file_path} \n\n"

        print(
            f"Successfully uploaded {output_file_path} to {remote_file_path}")

    finally:
        # Close the SFTP session and transport
        sftp.close()
        transport.close()


@app.get("/")
async def serve_homepage():
    return FileResponse("templates/index.html")
# uvicorn app:app --host 0.0.0.0 --port 8000 --reload


@app.get("/keka_sync")
async def stream_data():
    """ Stream process step by step """
    async def event_stream():
        yield f"data: Connecting to KEKA...... \n\n"
        access_token = await fetch_access_token()  # Await the async function
        if not access_token:
            yield "data: Failed to retrieve access token\n\n"
            return

        yield f"data: Connected to KEKA \n\n"

        employee_data = []
        async for message in call_second_api(access_token):
            # data = json.loads(message)
            # if "employees" in data:  # Final employee list
            #     employee_data = data["employees"]
            # yield message  # Stream messages
            try:
                data = json.loads(message)  # Attempt to parse message as JSON

                if isinstance(data, dict) and "employees" in data:  # Ensure it's a dictionary
                    employee_data = data["employees"]
                else:
                    yield message  # Stream messages as they arrive

            except json.JSONDecodeError:
                yield message

        if not employee_data:
            yield "data: No employees found\n\n"
            return
        yield f"data: Total Pages {len(employee_data)}\n\n"
        print("================", len(employee_data))
        # yield f"data: Total Records {len(employee_data)}\n"

        # employees_gen = call_second_api(access_token)
        # async for message in employees_gen:
        #     yield message

        async for upload_msg in upload_to_ftp(employee_data):
            yield upload_msg

    return StreamingResponse(event_stream(), media_type="text/event-stream")
