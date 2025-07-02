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

import re
from typing import Optional

from couchbase_analytics.common._core.utils import is_null_or_empty

# TODO: Apparently Go does not allow a leading decimal point without a leading zero, e.g., ".5s" is invalid.
#       We allowed this in the Columnar SDK due to how the C++ client parsed durations
DURATION_PATTERN = re.compile(r'^([-+]?)((\d*(\.\d*)?){1}(?:ns|us|µs|μs|ms|s|m|h){1})+$')
DURATION_PAIRS_PATTERN = re.compile(r'(\d*(?:\.\d*)?)(ns|us|ms|s|m|h)')

def check_valid_duration_str(duration_str: str) -> None:
    """
    Validates if the given string is a valid duration string.
    
    :param value: The duration string to validate.
    :return: True if valid, False otherwise.
    """
    if not isinstance(duration_str, str):
        raise ValueError(f'Expected a string, got {type(duration_str).__name__} instead.')
    
    if is_null_or_empty(duration_str):
        raise ValueError('Duration string cannot be empty.')
    
    if duration_str.startswith('-'):
        raise ValueError('Negative durations are not supported.')
    
    # Special case: "0" duration
    if duration_str == '0':
        return

    match = DURATION_PATTERN.fullmatch(duration_str)

    if not match:
        raise ValueError('Duration string has invalid format')
    
def parse_duration_str(duration_str: str, in_millis: Optional[bool]=False) -> float:
    check_valid_duration_str(duration_str)

    # Special case: "0" duration
    if duration_str == '0':
        return 0.0

    # Normalize 'µs' (micro)
    duration_str = duration_str.replace('µs', 'us').replace('μs', 'us')

    # Mapping of units to their multiplier to convert to seconds
    unit_multipliers = {
        'ns': 1e-9,  # nanoseconds
        'us': 1e-6,  # microseconds
        'ms': 1e-3,  # milliseconds
        's': 1.0,    # seconds
        'm': 60.0,   # minutes
        'h': 3600.0  # hours
    }

    segments = DURATION_PAIRS_PATTERN.findall(duration_str)
    total_seconds = 0.0
    for num_str, unit_str in segments:
        try:
            value = float(num_str)
            total_seconds += value * unit_multipliers[unit_str]
        except OverflowError as e:
            raise ValueError((f'Invalid duration. Overflow error while parsing number "{num_str}{unit_str}". '
                              f'Error details: {e}')) from None
        except ValueError as e:
            raise ValueError((f'Invalid duration. Parsing error while parsing number "{num_str}{unit_str}". '
                              f'Error details: {e}')) from None
        except KeyError:
            raise ValueError(f'Invalid duration.  Unknown unit "{unit_str}"') from None
        
    if in_millis:
        total_seconds *= 1e3
    return total_seconds