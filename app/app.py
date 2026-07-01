# Copyright © 2026 Bull® SAS. All rights reserved
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import Flask
from flask_restx import Api
from routes.workflow import api as wf_namespace
from flask_cors import CORS
import logging
import os
from dotenv import load_dotenv
from flask_swagger_ui import get_swaggerui_blueprint

# Load env vars
load_dotenv()
REDIRECT_URL = os.getenv('REDIRECT_URL')
TOKEN_URL= os.getenv('TOKEN_URL')
AUTH_URL = os.getenv('AUTH_URL')

def set_logger():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

authorizations = { 
		'OAuth2': {
			'type': 'oauth2',
			'flow': 'accessCode',
			'tokenUrl': TOKEN_URL,
			'authorizationUrl': AUTH_URL,
			'redirect_uri': REDIRECT_URL,
			'scopes': {
					'openid': 'Get ID token',
					'email': 'Get email',
					'profile': 'Get identity',
			}
		}
	 }

SWAGGER_URL="/swagger"
API_URL="/swagger.json"

swagger_ui_blueprint = get_swaggerui_blueprint(
		SWAGGER_URL,
		API_URL,
		config={
			'app_name': 'Job Configurator Access API'
		})

api = Api(
    title="Pistis Job Configurator REST API",
    version="1.0",
    description="API for Job Configurator",
    authorizations=authorizations,
    security='OAuth2')

api.add_namespace(wf_namespace, path='/workflow')
#api.add_namespace(login_namespace, path='/login')

flask_app =Flask(__name__)
flask_app.register_blueprint(swagger_ui_blueprint, url_prefix=SWAGGER_URL)
flask_app.config["RESTX_MASK_SWAGGER"]=False
flask_app.config['UPLOAD_EXTENSIONS'] = ['.tsv', '.parquet', '.json', '.csv', '.xlsx']
api.init_app(flask_app)
CORS(flask_app)

set_logger()

if __name__=="__main__":
    flask_app.run(debug=True, port=5000)

#if __name__ != '__main__':
#    gunicorn_logger = logging.getLogger('gunicorn.error')
#    flask_app.logger.handlers = gunicorn_logger.handlers
#    flask_app.logger.setLevel(gunicorn_logger.level)    