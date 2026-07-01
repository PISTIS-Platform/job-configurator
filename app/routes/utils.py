# Copyright © 2026 Bull® SAS. All rights reserved
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from flask import request
from functools import wraps

### service Names
DATA_CHECK_IN = "Data Check-In"
DATA_TRANSFORMATION = "Data Transformation"
DATA_TRANSFORMATION_NEW = "Transformation Designer"
INSIGHTS_GENERATOR = "Insights Generator"
DATA_CHECK_IN_FILE_METHOD = "uploadFile"
DATA_CHECK_IN_FTP_METHOD = "uploadDataFromFtpServer"
DATA_CHECK_IN_API_METHOD = "uploadDataFromAPI"

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None

        if 'authorization' in request.headers:
            token = request.headers.get('authorization')

        if not token or not is_bearer_token(token):
            return {'message' : 'Token is missing'}, 401

        return f(*args, **kwargs)
    return decorated

def is_bearer_token(token):
    return token.startswith("Bearer ")