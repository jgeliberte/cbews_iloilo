
"""
Contains server run configurations
"""


class Config(object):
    """
    Common configurations
    """
    JSON_SORT_KEYS = False

class DevelopmentConfig(Config):
    """
    Development configurations
    """

    DEBUG = True


class ProductionConfig(Config):
    """
    Production configurations
    """

    DEBUG = False


APP_CONFIG = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "url": "http://localhost:3000",
    "MARIRONG_DIR": "/var/www/html/CBEWSL/MARIRONG",
    "CANDIDATE_DIR": "/home/louie-cbews/CODES/cbews_iloilo/Documents/monitoringoutput/alertgen"
}
