"""
Main application file
Contains initialization lines for main project methods
"""

import datetime

from flask import Flask
from flask_login import LoginManager
from flask_cors import CORS
from flask_bcrypt import Bcrypt
from flask_socketio import SocketIO
from flask_jwt_extended import JWTManager
from pprint import pprint
from config import APP_CONFIG
BCRYPT = Bcrypt()
JWT = JWTManager()
SOCKETIO = SocketIO()

def create_app():
    """
    Instantiate Flask App variable and other related packages
    """

    app = Flask(__name__, instance_relative_config=True)

    app.config.from_pyfile("config.py")

    JWT.init_app(app)
    app.config['JWT_ACCESS_TOKEN_EXPIRES'] = datetime.timedelta(minutes=30)
    CORS(app)
    SOCKETIO.init_app(app)

    from src.api.users.user_account import USER_ACCOUNT_BLUEPRINT
    app.register_blueprint(USER_ACCOUNT_BLUEPRINT, url_prefix="/api")

    from src.api.users.user import USER_BLUEPRINT
    app.register_blueprint(USER_BLUEPRINT, url_prefix="/api")

    from src.api.risk_assessment.cav import CAPACITY_AND_VULNERABILITY_BLUEPRINT
    app.register_blueprint(CAPACITY_AND_VULNERABILITY_BLUEPRINT, url_prefix="/api")

    from src.api.risk_assessment.cra import COMMUNITY_RISK_ASSESSMENT_BLUEPRINT
    app.register_blueprint(COMMUNITY_RISK_ASSESSMENT_BLUEPRINT, url_prefix="/api")

    return app