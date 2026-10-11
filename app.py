import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(str(PROJECT_ROOT))

from web_server import app, init_services
init_services()

if __name__ == '__main__':
    import config
    app.run(host=config.WEB_HOST, port=config.WEB_PORT, debug=False, threaded=True)
