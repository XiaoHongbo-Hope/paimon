################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################

"""Collection of utilities about time intervals."""


def parse_duration(text: str) -> int:
    """
    Parse the given string to milliseconds.
    The string is in format "{length value}{time unit label}", e.g. "123ms", "321 s".
    If no time unit label is specified, it will be considered as milliseconds.
    
    Supported time unit labels are:
    - DAYS: "d", "day", "days"
    - HOURS: "h", "hour", "hours"
    - MINUTES: "m", "min", "minute", "minutes"
    - SECONDS: "s", "sec", "second", "seconds"
    - MILLISECONDS: "ms", "milli", "millisecond", "milliseconds"
    
    Args:
        text: String to parse (e.g., "10s", "5m", "100ms")
        
    Returns:
        Duration in milliseconds
        
    Raises:
        ValueError: If the text cannot be parsed
    """
    if text is None:
        raise ValueError("text cannot be None")

    trimmed = text.strip().lower()
    if not trimmed:
        raise ValueError("argument is an empty- or whitespace-only string")

    # Find the number part
    pos = 0
    while pos < len(trimmed) and (trimmed[pos].isdigit() or trimmed[pos] == '.'):
        pos += 1

    number_str = trimmed[:pos]
    unit_str = trimmed[pos:].strip()

    if not number_str:
        raise ValueError("text does not start with a number")

    try:
        value = float(number_str)
    except ValueError:
        raise ValueError(f"Cannot parse number from '{number_str}'")

    # Parse time unit (matching Java TimeUtils)
    unit_multipliers = {
        # Milliseconds
        'ms': 1,
        'milli': 1,
        'millisecond': 1,
        'milliseconds': 1,
        # Seconds
        's': 1000,
        'sec': 1000,
        'second': 1000,
        'seconds': 1000,
        # Minutes
        'm': 60 * 1000,
        'min': 60 * 1000,
        'minute': 60 * 1000,
        'minutes': 60 * 1000,
        # Hours
        'h': 60 * 60 * 1000,
        'hour': 60 * 60 * 1000,
        'hours': 60 * 60 * 1000,
        # Days
        'd': 24 * 60 * 60 * 1000,
        'day': 24 * 60 * 60 * 1000,
        'days': 24 * 60 * 60 * 1000,
    }

    if not unit_str:
        # Default to milliseconds if no unit specified (matching Java behavior)
        multiplier = 1
    elif unit_str in unit_multipliers:
        multiplier = unit_multipliers[unit_str]
    else:
        raise ValueError(
            f"Duration unit '{unit_str}' is not recognized. "
            f"Supported units: {', '.join(sorted(unit_multipliers.keys()))}"
        )

    result = int(value * multiplier)
    if result < 0:
        raise ValueError(f"Duration cannot be negative: {text}")

    return result






