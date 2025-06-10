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

# TODO: versioning
import sys

try:
    from couchbase_analytics._version import __version__
except ImportError:
    __version__ = '0.0.0-could-not-find-version'

PYCBAC_VERSION = f'pycbac/{__version__}'

try:
    python_version_info = sys.version.split(' ')
    if len(python_version_info) > 0:
        PYCBAC_VERSION = f'{PYCBAC_VERSION} (python/{python_version_info[0]})'
except Exception:  # nosec
    pass

"""

pycbac teardown methods

"""
# import atexit  # nopep8 # isort:skip # noqa: E402


# def _pycbac_teardown(**kwargs: object) -> None:
#     """**INTERNAL**"""
#     global _PYCBAC_LOGGER
#     if _PYCBAC_LOGGER:
#         # TODO:  see about synchronizing the logger's shutdown here
#         _PYCBAC_LOGGER = None  # type: ignore


# atexit.register(_pycbac_teardown)


"""

Logging methods

"""
# TODO: logging

# def configure_console_logger() -> None:
#     import os
#     log_level = os.getenv('PYCBAC_LOG_LEVEL', None)
#     if log_level:
#         _PYCBAC_LOGGER.create_console_logger(log_level.lower())
#         logger = logging.getLogger()
#         logger.info(f'Python Couchbase Analytics Client ({PYCBAC_VERSION})')
#         logging.getLogger().debug(get_metadata(as_str=True))


# def configure_logging(name: str,
#                       level: Optional[int] = logging.INFO,
#                       parent_logger: Optional[logging.Logger] = None) -> None:
#     if parent_logger:
#         name = f'{parent_logger.name}.{name}'
#     logger = logging.getLogger(name)
#     _PYCBAC_LOGGER.configure_logging_sink(logger, level)
#     logger.info(f'Python Couchbase Analytics Client ({PYCBAC_VERSION})')
#     logger.debug(get_metadata(as_str=True))


# configure_console_logger()
