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

from flask_restx import fields

class LoginModel:
    def __init__(self, namespace):
        self.namespace=namespace
    
    def login_expected_payload(self):
        data_model={
            "username":fields.String(),
            "password":fields.String(),
        }
        return self.namespace.model("login_expected_payload", data_model)

    def login(self):
        data_model={
            "data":fields.Nested(self._token()),
            "message":fields.String()
        }
        return self.namespace.model("login", data_model)
    
    def _token(self):
        data_model={
            "token":fields.String()
        }
        return self.namespace.model("token", data_model)