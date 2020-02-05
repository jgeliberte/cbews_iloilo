
from flask import Blueprint, jsonify, request
from connections import SOCKETIO

USER_BLUEPRINT = Blueprint("user_blueprint", __name__)

@USER_BLUEPRINT.route("/users/create", methods=["POST"])
def create():
    data = request.get_json()
    return jsonify({"status": "create"})

@USER_BLUEPRINT.route("/users/fetch/<user_id>", methods=["GET"])
def fetch(user_id=0):
    #user_id 0 = All users fetched
    print(user_id)
    return jsonify({"status": "fetch"})

@USER_BLUEPRINT.route("/users/modify/", methods=["PATCH"])
def modify():
    data = request.get_json()
    print(data)
    return jsonify({"status": "modify"})

@USER_BLUEPRINT.route("/users/purge/", methods=["DELETE"])
def purge():
    data = request.get_json()
    print(data)
    return jsonify({"status": "purge"})