
from flask import Blueprint, jsonify, request
from src.model.users import Users
import hashlib

USER_ACCOUNT_BLUEPRINT = Blueprint("user_account_blueprint", __name__)

@USER_ACCOUNT_BLUEPRINT.route("/accounts/signin", methods=["POST"])
def signin():
    credentials = request.get_json()
    username, password = credentials.values()
    account_details = Users.fetch_account(username)
    if len(account_details) != 0:
        print(account_details[0])
        (user_id, account_id, privilege_id, profile_id, v_username, v_password, salt, firstname, lastname, site_id, site_code) = account_details[0]
        password_hashed = str(hashlib.sha512(str(password+salt).encode("utf-8")).hexdigest())

        if v_password == password_hashed:
            status = {
                "status": True,
                "message": "Login Successfull!",
                "user_data": {
                    "account_id": account_id,
                    "user_id": user_id,
                    "privilege_id": privilege_id,
                    "profile_id": profile_id,
                    "site_id": site_id,
                    "site_code": site_code,
                    "firstname": firstname,
                    "lastname": lastname
                }
            }
        else:
            status = {
                "status": False,
                "message": "Invalid password, please try again."
            }
    else:
        status = {
            "status": False,
            "message": "Username / Password does not match any records."
        }
    return jsonify(status)

@USER_ACCOUNT_BLUEPRINT.route("/accounts/signout", methods=["GET"])
def signout():
    return jsonify({"status": True})

@USER_ACCOUNT_BLUEPRINT.route("/accounts/signup", methods=["POST"])
def signup():
    credentials = request.get_json()
    is_existing = Users.account_exists(credentials['username'])[0][0]
    if is_existing == 0:
        status = Users.create_user_account(credentials)
    else:
        status = False
    return jsonify({"status": status})

@USER_ACCOUNT_BLUEPRINT.route("/accounts/forgot_password", methods=["GET"])
def forgot_password():
    return jsonify({"status": True})