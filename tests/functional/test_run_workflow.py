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

from app.app import flask_app 

def test_workflow_run():
    """"
    GIVEN a Flask application configured for testing 
    WHEN the '/workflow/run' page is posted to (POST)
    THEN check that the workflow is running properly
    """

    response = flask_app.test_client().post('/workflow/run',
                                data = {
                                    "job_list": [
                                        {
                                        "source": "https://",
                                        "endpoint": "ws://",
                                        "input_data": {
                                            "additionalProp1": "string",
                                            "additionalProp2": "string",
                                            "additionalProp3": "string"
                                        },
                                        "method": "get",
                                        "destination_type": "memory"
                                        }
                                    ]
                                },
                                follow_redirects=True)
    assert response.status_code == 200