"""
File Runner of the whole server
Contains the Flask App initialization function
"""

import os
from connections import create_app, SOCKETIO

APP = create_app()

if __name__ == "__main__":
    print("Flask server is now running...")
    SOCKETIO.run(APP, host="0.0.0.0", port=5000,
                 debug=True, use_reloader=False)
