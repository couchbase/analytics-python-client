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

from __future__ import annotations

from enum import Enum


class QueryScanConsistency(Enum):
    """
    Represents the various scan consistency options that are available.
    """

    NOT_BOUNDED = 'not_bounded'
    REQUEST_PLUS = 'request_plus'


# This is unfortunate, but Enum is 'special' and this is one of the least invasive manners to document the members
QueryScanConsistency.NOT_BOUNDED.__doc__ = ('Indicates that no specific consistency is required, '
                                            'this is the fastest options, but results may not include '
                                            'the most recent operations which have been performed.')
QueryScanConsistency.REQUEST_PLUS.__doc__ = ('Indicates that the results to the query should include '
                                             'all operations that have occurred up until the query was started. '
                                             'This incurs a performance penalty of waiting for the index to catch '
                                             'up to the most recent operations, but provides the highest level '
                                             'of consistency.')
