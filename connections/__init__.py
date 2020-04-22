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

    from src.api.ground_data.surficial_markers import SURFICIAL_MARKERS_BLUEPRINT
    app.register_blueprint(SURFICIAL_MARKERS_BLUEPRINT, url_prefix="/api")

    from src.api.ground_data.manifestation_of_movements import MANIFESTATION_OF_MOVEMENTS_BLUEPRINT
    app.register_blueprint(MANIFESTATION_OF_MOVEMENTS_BLUEPRINT, url_prefix="/api")

    from src.api.alert_generation.public_alerts import PUBLIC_ALERTS_BLUEPRINT
    app.register_blueprint(PUBLIC_ALERTS_BLUEPRINT, url_prefix="/api")

    from src.api.data_analysis.rainfall_analysis import RAINFALL_ANALYSIS_BLUEPRINT
    app.register_blueprint(RAINFALL_ANALYSIS_BLUEPRINT, url_prefix="/api")

    from src.api.data_analysis.surficial_analysis import SURFICIAL_ANALYSIS_BLUEPRINT
    app.register_blueprint(SURFICIAL_ANALYSIS_BLUEPRINT, url_prefix="/api")

    from src.api.data_analysis.subsurface_analysis import SUBSURFACE_ANALYSIS_BLUEPRINT
    app.register_blueprint(SUBSURFACE_ANALYSIS_BLUEPRINT, url_prefix="/api")

    from src.api.sensor_data.earthquake import EARTHQUAKE_BLUEPRINT
    app.register_blueprint(EARTHQUAKE_BLUEPRINT, url_prefix="/api")

    from src.api.maintenance.maintenance_logs import MAINTENANCE_LOGS_BLUEPRINT
    app.register_blueprint(MAINTENANCE_LOGS_BLUEPRINT, url_prefix="/api")

    from src.api.maintenance.incident_reports import INCIDENT_REPORTS_BLUEPRINT
    app.register_blueprint(INCIDENT_REPORTS_BLUEPRINT, url_prefix="/api")

    from src.api.reports import REPORTS_BLUEPRINT
    app.register_blueprint(REPORTS_BLUEPRINT, url_prefix="/api")

    from src.api.events.template_creator import TEMPLATE_CREATOR_BLUEPRINT
    app.register_blueprint(TEMPLATE_CREATOR_BLUEPRINT, url_prefix="/api")

    from src.api.ground_data.on_demand import ON_DEMAND_BLUEPRINT
    app.register_blueprint(ON_DEMAND_BLUEPRINT, url_prefix="/api")

    from src.api.risk_assessment.hazard_maps import HAZARD_MAPS_BLUEPRINT
    app.register_blueprint(HAZARD_MAPS_BLUEPRINT, url_prefix="/api")
    
    from src.api.test import TEST_BLUEPRINT
    app.register_blueprint(TEST_BLUEPRINT, url_prefix="/test")
    
    return app