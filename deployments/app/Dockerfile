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

FROM python:3.11-bullseye
WORKDIR /opt/pistis-job-configurator

ENV http_proxy=http://10.197.209.8:3128
ENV https_proxy=http://10.197.209.8:3128
ENV no_proxy=localhost,127.0.0.1,.default.svc.cluster.local,.pistis-market.eu

RUN apt-get update
RUN pip install poetry gunicorn

COPY app ./app
COPY templates ./app/templates
COPY tests ./tests 
COPY pyproject.toml README.md LICENSE ./

RUN make -C ./app install 
#&& make -C ./app test

ENTRYPOINT ["gunicorn", "--log-level", "INFO", "-b", ":8295", "-t", "120", "app.app:flask_app"]
