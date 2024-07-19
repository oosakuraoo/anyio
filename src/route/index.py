from flask import Blueprint, jsonify
from src.model.res.base import ObjData

index_api = Blueprint("index", __name__)


@index_api.route("", methods=["GET"])
def index():
    return jsonify(ObjData().to_json())
