#  Copyright 2016-2024. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import sys

from setuptools import find_packages, setup

sys.path.append('.')
import couchbase_analytics_version  # nopep8 # isort:skip # noqa: E402

try:
    couchbase_analytics_version.gen_version()
except couchbase_analytics_version.CantInvokeGit:
    pass

PYCBAC_README = os.path.join(os.path.dirname(__file__), 'README.md')
PYCBAC_VERSION = couchbase_analytics_version.get_version()


package_data = {
    'couchbase_analytics.common.core._nonprod_certificates': ['*.pem'],
    'couchbase_analytics.common.core._capella_certificates': ['*.pem'],
}

print(f'Python Analytics SDK version: {PYCBAC_VERSION}')

setup(
    name='couchbase-analytics',
    version=PYCBAC_VERSION,
    python_requires='>=3.9',
    install_requires=['httpx~=0.28.1', 'ijson~=3.3.0', 'typing-extensions~=4.11; python_version<"3.11"'],
    packages=find_packages(
        include=['acouchbase_analytics', 'couchbase_analytics', 'acouchbase_analytics.*', 'couchbase_analytics.*'],
        exclude=['acouchbase_analytics.tests', 'couchbase_analytics.tests'],
    ),
    package_data=package_data,
    long_description=open(PYCBAC_README, 'r').read(),
    long_description_content_type='text/markdown',
)
