#!/usr/bin/env python3

from urllib import request, parse
import json
from mysql.connector import connect, Error
from datetime import datetime


def create_and_write_new_upload(cursor) -> str:
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("INSERT INTO Upload (id) VALUES (%s)", (timestamp,))
    return timestamp


def write_json_to_db(cursor, table: str, json_data: dict, timestamp="") -> None:
    if table not in ["Reasons_For_Leaving", "Lost_Prospect_Reasons", "Request_Params", "Load"]:
        if timestamp == "":
            raise Exception("must pass timestamp as parameter")
        json_data["upload_id"] = timestamp
    # write json to db
    sql = "INSERT INTO " + table + "("
    keys = list(json_data.keys())
    values = []
    # to preserve order
    for i in range(len(keys)):
        values.append(json_data[keys[i]])
    for i in range(len(keys) - 1):
        sql += keys[i] + ", "
    sql += keys[-1] + ") VALUES ("
    for i in range(len(keys) - 1):
        sql += "%s, "
    sql += "%s"
    sql += ")"
    # print(sql)
    # print(tuple(values))

    cursor.execute(sql, tuple(values))


def get(url: str, extra_params={}) -> dict:
    req = request.Request('https://api.myresman.com/' + url, method="POST")
    req.add_header('Content-Type', 'application/x-www-form-urlencoded')
    req.add_header('Accept', 'application/json')

    data = {
        "IntegrationPartnerID": "INSERT INTEGRATION PARTNER ID HERE",
        "ApiKey": "INSERT RESMAN API KEY HERE",
        "AccountID": "INSERT RESMAN ACCOUNT ID HERE"
    }

    data = {**data, **extra_params}

   #  print("URL: " + 'https://api.myresman.com/' + url)
   #  print("Params: ")
   # print(data)

    data = parse.urlencode(data).encode()

    r = request.urlopen(req, data=data)
    content = r.read()

    json_data = json.loads(content.decode('utf-8'))
    return json_data


def sanitize_property(property: dict) -> dict:
    sanitized_json = {
        "resman_id": property["PropertyID"],
        "name": property["Name"],
        "street_address": property["StreetAddress"],
        "city": property["City"],
        "state": property["State"],
        "zip": property["Zip"],
        "phone": property["Phone"],
        "email": property["Email"],
    }
    # this isn't always there for some reason
    if "Manager" in property:
        sanitized_json["manager"] = property["Manager"]

    return sanitized_json


def load_properties(db, cursor, timestamp) -> None:
    def get_property_id(resman_id):
        query = "SELECT id from Property WHERE resman_id=\'" + resman_id + "\' AND upload_id=\'" + timestamp + "\'"
         # print(query)
        cursor.execute(query)
        id = cursor.fetchall()[0][0]  # should only be one
        # print(id)
        return id

    json_properties = get("Account/GetProperties")["Properties"]
    prop_ids = []
    for property in json_properties:
        sanitized_json_property = sanitize_property(property)
        write_json_to_db(cursor, "Property", sanitized_json_property, timestamp=timestamp)
        db.commit()
        prop_ids.append({
            "property_id": get_property_id(sanitized_json_property["resman_id"]),
            "resman_id": sanitized_json_property["resman_id"]
        })
    return prop_ids


def sanitize_work_order(work_order: dict) -> dict:
    sanitized_json = {
        "resman_id": work_order["WorkOrderID"],
        "property_id": work_order["PropertyID"],
        "account_id": work_order["AccountID"],
        "assigned_to": work_order["AssignedTo"],
        "resman_assigned_to_person_id": work_order["AssignedToPersonID"],
        "resman_category_id": work_order["CategoryID"],
        "status": work_order["Status"],
        "reported_date": work_order["ReportedDate"],
        "due_date": work_order["DueDate"],
    }

    if "Unit" in work_order:
        sanitized_json["unit"] = work_order["Unit"]
    if "Cost" in work_order:
        sanitized_json["cost"] = work_order["Cost"]
    if "ReportedBy" in work_order:
        sanitized_json["reported_by"] = work_order["ReportedBy"]
    if "Unit" in work_order:
        sanitized_json["unit"] = work_order["Unit"]
    if "Appointment" in work_order:
        sanitized_json["appointment"] = work_order["Appointment"]
    if "Phone" in work_order:
        sanitized_json["phone"] = work_order["Phone"]
    if "Notes" in work_order:
        sanitized_json["notes"] = work_order["Notes"]
    if "CompletedNotes" in work_order:
        sanitized_json["completed_notes"] = work_order["CompletedNotes"]

    return sanitized_json


def load_work_orders(cursor, property_id, resman_property_id, timestamp) -> None:
    # json_work_orders = get_work_orders(resman_property_id)
    json_work_orders = get("WorkOrders/GetWorkOrders", {"PropertyID": resman_property_id})
    json_work_orders_final = json_work_orders["WorkOrders"]
    account_id = 2  
    # print(account_id)
    for work_order in json_work_orders_final:
        work_order["AccountID"] = account_id
        work_order["PropertyID"] = property_id  # overwrite resman ID
        sanitized_json_work_order = sanitize_work_order(work_order)
        write_json_to_db(cursor, "Work_Order", sanitized_json_work_order, timestamp=timestamp)


def sanitize_unit(unit: dict) -> dict:
    sanitized_json = {}
    mapping = {
        "UnitNumber": "unit_number",
        "UnitType": "unit_type",
        "Building": "building",
        "Floor": "floor",
        "StreetAddress": "street_address",
        "City": "city",
        "State": "state",
        "Zip": "zip",
        "AccountID": "account_id",
        "PropertyID": "property_id",
    }

    for key in mapping:
        if key in unit:
            sanitized_json[mapping[key]] = unit[key]

    return sanitized_json


def load_units(cursor, property_id, resman_property_id, timestamp) -> None:
    # json_work_orders = get_work_orders(resman_property_id)
    json_units = get("Property/GetUnits", {"PropertyID": resman_property_id})
    json_units_final = json_units["Units"]
    account_id = 2  
    # print(account_id)
    for work_order in json_units_final:
        work_order["AccountID"] = account_id
        work_order["PropertyID"] = property_id  # overwrite resman ID
        sanitized_json_unit = sanitize_unit(work_order)
        write_json_to_db(cursor, "Unit", sanitized_json_unit, timestamp=timestamp)


def sanitize_resident(resident: dict) -> dict:
    sanitized_json = {}
    mapping = {
        "FirstName": "first_name",
        "LastName": "last_name",
        "Building": "building",
        "Email": "email",
        "MobilePhone": "mobile_phone",
        "HomePhone": "home_phone",
        "WorkPhone": "work_phone",
        "LeaseStartDate": "lease_start_date",
        "LeaseEndDate": "lease_end_date",
        "MoveInDate": "move_in_date",
        "MoveOutDate": "move_out_date",
        "HouseholdStatus": "household_status",
        "MainContact": "main_contact",
        "isMinor": "is_minor",
        "AccountID": "account_id",
        "PropertyID": "property_id",
    }

    for key in mapping:
        if key in resident:
            sanitized_json[mapping[key]] = resident[key]

    return sanitized_json


def load_residents(cursor, property_id, resman_property_id, timestamp) -> None:
    # json_work_orders = get_work_orders(resman_property_id)
    json_residents = get("Leasing/GetCurrentResidents", {"PropertyID": resman_property_id})
    json_residents_final = json_residents["Residents"]
    account_id = 2 
    # print(account_id)
    for resident in json_residents_final:
        resident["AccountID"] = account_id
        resident["PropertyID"] = property_id  # overwrite resman ID
        sanitized_json_resident = sanitize_resident(resident)
        write_json_to_db(cursor, "Resident", sanitized_json_resident, timestamp=timestamp)


def sanitize_lease(lease: dict) -> dict:
    sanitized_json = {}
    mapping = {
        "FirstName": "first_name",
        "LastName": "last_name",
        "StreetAddress": "street_address",
        "UnitNumber": "unit_number",
        "City": "city",
        "State": "state",
        "LeaseEndDate": "lease_end_date",
        "Status": "status",
        "AccountID": "account_id",
        "PropertyID": "property_id",
    }

    for key in mapping:
        if key in lease:
            sanitized_json[mapping[key]] = lease[key]

    return sanitized_json


def load_leases(cursor, property_id, resman_property_id, timestamp) -> None:
    # fragile attemptto get all leases
    json_leases = get("Events/GetLeaseExpirations",
                      {"PropertyID": resman_property_id, "StartDate": "1800-01-01", "EndDate": "2200-01-01"})
    json_leases_final = json_leases["Leases"]
    account_id = 2
    # print(account_id)
    for lease in json_leases_final:
        lease["AccountID"] = account_id
        lease["PropertyID"] = property_id  # overwrite resman ID
        sanitized_json_lease = sanitize_lease(lease)
        write_json_to_db(cursor, "Lease", sanitized_json_lease, timestamp=timestamp)


def sanitize_gl_account(gl_account: dict) -> dict:
    sanitized_json = {}
    mapping = {
        "Name": "name",
        "Number": "number",
        "Type": "type",
        "AccountID": "account_id",
        "PropertyID": "property_id",
    }

    for key in mapping:
        if key in gl_account:
            sanitized_json[mapping[key]] = gl_account[key]

    return sanitized_json


def sanitize_period(period: dict) -> dict:
    sanitized_json = {}
    mapping = {
        "Month": "month",
        "Year": "year",
        "Actual": "actual",
        "Budget": "budget",
        "gl_account_id": "gl_account_id"
    }

    for key in mapping:
        if key in period:
            sanitized_json[mapping[key]] = period[key]

    return sanitized_json


def load_gl_accounts(cursor, property_id, resman_property_id, timestamp) -> None:
    def get_most_recent_account():
        query = "SELECT MAX(id) from GL_Account"
        # print(query)
        cursor.execute(query)
        id = cursor.fetchall()[0][0]  # should only be one
        # print(id)
        return id

    # fragile attemptto get all results
    json_gl_accounts = get("Accounting/GetBudgetAndActual",
                           {"PropertyID": resman_property_id, "StartMonth": "2019-01-01", "EndMonth": "2020-01-01"})
    # print(json_gl_accounts)
    json_gl_accounts_final = json_gl_accounts["GLAccounts"]
    account_id = 2 
    # print(account_id)
    for gl_account in json_gl_accounts_final:
        gl_account["AccountID"] = account_id
        gl_account["PropertyID"] = property_id  # overwrite resman ID
        sanitized_json_gl_account = sanitize_lease(gl_account)
        write_json_to_db(cursor, "GL_Account", sanitized_json_gl_account, timestamp=timestamp)
        gl_account_id = get_most_recent_account()
        periods = gl_account["Periods"]
        for period in periods:
            period["gl_account_id"] = gl_account_id
            sanitized_json_period = sanitize_period(period)
            write_json_to_db(cursor, "GL_Account_Periods", sanitized_json_period, timestamp=timestamp)


def main():
    try:
        # setup db connection
        db = connect(host="INSERT URL TO RDS DATABASE HERE", user="INSERT DATABASE USERNAME HERE", password="INSERT DATABASE PASSWORD HERE",
                     database="INSERT DATABASE NAME HERE")
        cursor = db.cursor()

        timestamp = create_and_write_new_upload(cursor)

        properties = load_properties(db, cursor, timestamp)

        # print(get("MITS/SearchProspects", {"PropertyID": properties[0]["property_id"]}))

        for prop in properties:
            load_work_orders(cursor, prop["property_id"], prop["resman_id"], timestamp)
            load_units(cursor, prop["property_id"], prop["resman_id"], timestamp)
            load_residents(cursor, prop["property_id"], prop["resman_id"], timestamp)
            load_leases(cursor, prop["property_id"], prop["resman_id"], timestamp)
            load_gl_accounts(cursor, prop["property_id"], prop["resman_id"], timestamp)

        db.commit()

    except Error as e:
        db.rollback()
        print(e)
        x = 0
    finally:

        cursor.close()
        db.close()
        print("Program finished")


if __name__ == "__main__":
    main()