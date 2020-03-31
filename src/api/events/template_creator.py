
from flask import Blueprint, jsonify, request
from src.model.users import Users
import hashlib
from src.model.template_creator import TemplateCreator

TEMPLATE_CREATOR_BLUEPRINT = Blueprint("template_creator_blueprint", __name__)

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/add", methods=["POST"])
def add():
    ewi_data = request.get_json()
    return jsonify({'status': True})

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/delete", methods=["DELETE"])
def delete():
    ewi_id = request.get_json()
    return jsonify({"status": True})

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/update", methods=["PATCH"])
def update():
    ewi_data = request.get_json()
    return jsonify({"status": True})

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/fetch/<ewi_id>", methods=["GET"])
def fetch(ewi_id):
    return jsonify({"status": True})

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/fetch/all", methods=["GET"])
def fetch_all():
    ret_val = TemplateCreator.fetch()
    return jsonify({"status": True, 'data': ret_val})