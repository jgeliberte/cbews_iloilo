
from flask import Blueprint, jsonify, request
from src.model.users import Users
import hashlib
from src.model.template_creator import TemplateCreator

TEMPLATE_CREATOR_BLUEPRINT = Blueprint("template_creator_blueprint", __name__)

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/add", methods=["POST"])
def add():
    ewi_data = request.get_json()
    ret_val = TemplateCreator.add(ewi_data)
    if isinstance(int(ret_val), int) == True:
        status = {'status': True}
    else:
        status = {'status': False} 
    return jsonify(status)

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/delete", methods=["DELETE"])
def delete():
    ewi_data = request.get_json()
    ret_val = TemplateCreator.delete(ewi_data['ewi_id'])
    if int(ret_val) == 0:
        status = {"status": True}
    else:
        status = {"status": False}
    return jsonify(status)

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/update", methods=["PATCH"])
def update():
    ewi_data = request.get_json()
    ret_val = TemplateCreator.update(ewi_data)
    if int(ret_val) == 0:
        status = {"status": True}
    else:
        status = {"status": False}
    return jsonify(status)

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/fetch/<ewi_id>", methods=["GET"])
def fetch(ewi_id):
    return jsonify({"status": True})

@TEMPLATE_CREATOR_BLUEPRINT.route("/events/template/fetch/all", methods=["GET"])
def fetch_all():
    ret_val = TemplateCreator.fetch()
    return jsonify({"status": True, 'data': ret_val})