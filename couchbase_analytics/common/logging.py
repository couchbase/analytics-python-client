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
from enum import Enum

LOG_FORMAT_ARR = [
    '[%(asctime)s.%(msecs)03d]',
    '%(relativeCreated)dms',
    '[%(levelname)s]',
    '[%(process)d, %(threadName)s (%(thread)d)] %(name)s',
    '- %(message)s',
]
LOG_FORMAT = ' '.join(LOG_FORMAT_ARR)
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


def log_message(logger: logging.Logger, message: str, log_level: LogLevel) -> None:
    if not logger or not logger.hasHandlers():
        return

    if log_level == LogLevel.DEBUG:
        logger.debug(message)
    elif log_level == LogLevel.INFO:
        logger.info(message)
    elif log_level == LogLevel.WARNING:
        logger.warning(message)
    elif log_level == LogLevel.ERROR:
        logger.error(message)
    elif log_level == LogLevel.CRITICAL:
        logger.critical(message)
