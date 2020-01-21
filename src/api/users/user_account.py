
from flask import Blueprint, jsonify, request
from connections import SOCKETIO

USER_ACCOUNT_BLUEPRINT = Blueprint("user_account_blueprint", __name__)

@USER_ACCOUNT_BLUEPRINT.route("/accounts/signin", methods=["POST"])
def signin():
    return jsonify({"status": True})

@USER_ACCOUNT_BLUEPRINT.route("/accounts/signout", methods=["GET"])
def signout():
    return jsonify({"status": True})

@USER_ACCOUNT_BLUEPRINT.route("/accounts/signup", methods=["POST"])
def signup():
    return jsonify({"status": True})

@USER_ACCOUNT_BLUEPRINT.route("/accounts/forgot_password", methods=["GET"])
def forgot_password():
    return jsonify({"status": True})