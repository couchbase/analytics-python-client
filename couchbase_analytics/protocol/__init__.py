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

import logging
import sys

try:
    from couchbase_analytics._version import __version__  # type: ignore[import-not-found, unused-ignore]
except ImportError:
    __version__ = '0.0.0-could-not-find-version'

PYCBAC_VERSION = f'pycbac/{__version__}'

try:
    python_version_info = sys.version.split(' ')
    if len(python_version_info) > 0:
        PYCBAC_VERSION = f'{PYCBAC_VERSION} (python/{python_version_info[0]})'
except Exception:  # nosec
    pass


def configure_logger() -> None:
    import os

    log_level = os.getenv('PYCBAC_LOG_LEVEL', None)
    handlers_setup = logging.getLogger().hasHandlers()
    if log_level is not None or handlers_setup:
        logger = logging.getLogger()
        if not handlers_setup:
            from couchbase_analytics.common.logging import LOG_DATE_FORMAT, LOG_FORMAT

            log_level = log_level or 'INFO'
            logging.basicConfig(format=LOG_FORMAT, datefmt=LOG_DATE_FORMAT, level=log_level.upper())
        logger.info(f'Python Couchbase Analytics Client ({PYCBAC_VERSION})')


configure_logger()
