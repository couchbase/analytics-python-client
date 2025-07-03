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

from collections import deque
from enum import Enum
from typing import Deque, NamedTuple, Optional


class ParsingState(Enum):
    PROCESSING = 'processing'
    START_RESULTS_PROCESSING = 'start_results_processing'
    PROCESSING_RESULTS = 'processing_results'
    PROCESSING_RESULT = 'processing_result'
    START_ERRORS_PROCESSING = 'start_errors_processing'
    PROCESSING_ERRORS = 'processing_errors'
    PROCESSING_ERROR = 'processing_error'
    UNDEFINED = 'undefined'

    @staticmethod
    def okay_to_emit(state: ParsingState, previous_state: ParsingState) -> bool:
        if state == ParsingState.PROCESSING_RESULTS:
            return True
        return previous_state == ParsingState.PROCESSING_RESULTS and state == ParsingState.PROCESSING

    @staticmethod
    def should_pop_results_key(state: ParsingState, previous_state: ParsingState) -> bool:
        return previous_state == ParsingState.PROCESSING_RESULTS and state == ParsingState.PROCESSING

    def __str__(self) -> str:
        return self.value


class TokenState(Enum):
    RESULTS_START = 'results_start'
    RESULT_START = 'result_start'
    ERRORS_START = 'errors_start'
    ERROR_START = 'error_start'
    UNDEFINED = 'undefined'

    def __str__(self) -> str:
        return self.value

class TokenType(Enum):
    START_MAP = 'start_map'
    END_MAP = 'end_map'
    START_ARRAY = 'start_array'
    END_ARRAY = 'end_array'
    MAP_KEY = 'map_key'
    STRING = 'string'
    BOOLEAN = 'boolean'
    NULL = 'null'
    INTEGER = 'integer'
    DOUBLE = 'double'
    NUMBER = 'number'
    PAIR = 'pair'
    VALUE = 'value'
    OBJECT = 'object'
    UNKNOWN = 'unknown'

    @classmethod
    def from_str(cls, value: str) -> TokenType:
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(f'Invalid token type: {value}') from None

    def __str__(self) -> str:
        return self.value

class Token(NamedTuple):
    type: TokenType
    value: str
    state: Optional[TokenState]=None

VALUE_TOKENS = [TokenType.STRING,
                TokenType.BOOLEAN,
                TokenType.NULL,
                TokenType.INTEGER,
                TokenType.DOUBLE,
                TokenType.NUMBER]

EVENT_TOKENS = {
    TokenType.START_ARRAY: Token(TokenType.START_ARRAY, '['),
    TokenType.END_ARRAY: Token(TokenType.END_ARRAY, ']'),
    TokenType.START_MAP: Token(TokenType.START_MAP, '{'),
    TokenType.END_MAP: Token(TokenType.END_MAP, '}'),
}

POP_EVENTS = [TokenType.END_ARRAY, TokenType.END_MAP]

START_EVENTS = [TokenType.START_ARRAY, TokenType.START_MAP]

START_EVENT_TRANSITION_STATES = [ParsingState.START_RESULTS_PROCESSING,
                                 ParsingState.START_ERRORS_PROCESSING,
                                 ParsingState.PROCESSING_RESULTS]

class JsonTokenParserBase:
    def __init__(self, emit_results_enabled: bool) -> None:
        self._stack: Deque[Token] = deque()
        self._state = ParsingState.PROCESSING
        self._previous_state = ParsingState.UNDEFINED
        self._emit_results_enabled = emit_results_enabled
        self._results_type = TokenType.UNKNOWN
        self._has_errors = False

    @property
    def has_errors(self) -> bool:
        return self._has_errors

    @property
    def results_type(self) -> TokenType:
        return self._results_type

    def _check_results_in_raw_array(self) -> None:
        if self._results_type != TokenType.UNKNOWN:
            return
        if self._state == ParsingState.PROCESSING:
            return
        if self._state == ParsingState.PROCESSING_RESULTS:
            self._results_type = TokenType.VALUE
        else:
            self._results_type = TokenType.OBJECT

    def _get_matching_token(self, token_type: TokenType) -> Token:
        if token_type == TokenType.END_ARRAY:
            return EVENT_TOKENS[TokenType.START_ARRAY]
        elif token_type == TokenType.END_MAP:
            return EVENT_TOKENS[TokenType.START_MAP]
        else:
            raise ValueError(f'Invalid token type (cannot match): {token_type}')

    def _handle_map_key_token(self, value: str) -> None:
        if self._state == ParsingState.PROCESSING:
            if value == 'results':
                self._state = ParsingState.START_RESULTS_PROCESSING
                self._previous_state = ParsingState.PROCESSING
            elif value == 'errors':
                self._has_errors = True
                self._state = ParsingState.START_ERRORS_PROCESSING
                self._previous_state = ParsingState.PROCESSING
        self._push(TokenType.MAP_KEY, f'"{value}"')

    def _handle_pop_transition(self, token_state: Optional[TokenState]=None) -> bool:
        if token_state is not None:
            if token_state == TokenState.RESULTS_START:
                self._previous_state = self._state
                self._state = ParsingState.PROCESSING
            elif token_state == TokenState.ERRORS_START:
                self._previous_state = self._state
                self._state = ParsingState.PROCESSING
            elif token_state == TokenState.RESULT_START:
                self._previous_state = self._state
                self._state = ParsingState.PROCESSING_RESULTS
                return True
        return False

    def _handle_push_transition(self) -> Optional[TokenState]:
        if self._state == ParsingState.START_RESULTS_PROCESSING:
            self._previous_state = self._state
            self._state = ParsingState.PROCESSING_RESULTS
            return TokenState.RESULTS_START
        elif self._state == ParsingState.START_ERRORS_PROCESSING:
            self._previous_state = self._state
            self._state = ParsingState.PROCESSING_ERRORS
            return TokenState.ERRORS_START
        elif self._state == ParsingState.PROCESSING_RESULTS:
            self._previous_state = self._state
            self._state = ParsingState.PROCESSING_RESULT
            return TokenState.RESULT_START
        elif self._state == ParsingState.PROCESSING_ERRORS:
            self._previous_state = self._state
            self._state = ParsingState.PROCESSING_ERROR
            return TokenState.ERROR_START
        raise ValueError(f'Invalid state for push transition: {self._state}')

    def _handle_start_event(self, token_type: TokenType) -> None:
        transition = False
        if self._state in START_EVENT_TRANSITION_STATES:
            transition = True

        self._push(token_type, EVENT_TOKENS[token_type].value, transition)

    def _handle_value_token(self, token_type: TokenType, value: str) -> Optional[str]:
        self._check_results_in_raw_array()
        pair_key = val = None
        if len(self._stack) > 0 and self._stack[-1].type == TokenType.MAP_KEY:
            # no state transitions for a map_key token
            pair_key = self._pop().value
        if token_type == TokenType.STRING:
            if '"' in value:
                value = value.replace('"', '\\"')
            if "\\'" in value:
                value = value.replace("\\'", "\\\\'")
            val = f'"{value}"'
        elif token_type == TokenType.NULL:
            val = 'null'
        elif token_type == TokenType.BOOLEAN:
            val = f'{value}'.lower()
        else:
            val = f'{value}'
        if pair_key is not None:
            if self.results_type == TokenType.VALUE and self._state != ParsingState.PROCESSING:
                raise RuntimeError('JsonTokenParser: Cannot return value when pair key is present.')
            self._push(TokenType.PAIR, f'{pair_key}:{val}')
        else:
            if self._emit_results_enabled is True and self.results_type == TokenType.VALUE:
                return val
            self._push(TokenType.VALUE, val)
        return None

    def _push(self, token_type: TokenType, value: str, transition: Optional[bool]=False) -> None:
        token_state = None
        if transition is True:
            token_state = self._handle_push_transition()

        self._stack.append(Token(token_type, value, token_state))

    def _pop(self) -> Token:
        if self._stack:
            return self._stack.pop()
        raise ValueError('Stack is empty')

    def _should_push_pair(self, token: Token) -> bool:
        # when a results object is complete, the state will have transactioned back to PROCESSING
        # if we are not emitting rows or errors, we want to keep the results/errors object on the stack
        if (self._previous_state == ParsingState.PROCESSING_RESULTS
            and self._state == ParsingState.PROCESSING
            and self._emit_results_enabled is False):
            return True

        # the initial results object token will have a state of RESULTS_START
        # and we don't want to push them onto the stack
        if token.state != TokenState.RESULTS_START:
            return True

        return False
