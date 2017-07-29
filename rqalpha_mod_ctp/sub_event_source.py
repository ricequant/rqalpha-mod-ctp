# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
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

import time
import json
import redis
import datetime
from time import sleep
from threading import Thread
from collections import defaultdict
from redis.exceptions import RedisError
from dateutil.parser import parse
from six import iteritems

from rqalpha.environment import Environment
from rqalpha.model.tick import Tick
from rqalpha.utils.logger import system_log
from rqalpha.events import EVENT, Event

EVENT_DO_SEND_FEED = 'event_do_send_feed'


def convert_int_to_datetime_with_ms(dt_int):
    dt_int = int(dt_int)
    year, r = divmod(dt_int, 10000000000000)
    month, r = divmod(r, 100000000000)
    day, r = divmod(r, 1000000000)
    hour, r = divmod(r, 10000000)
    minute, r = divmod(r, 100000)
    second, millisecond = divmod(r, 1000)

    return datetime.datetime(year, month, day, hour, minute, second, millisecond * 1000)


class SubEvnetSource(object):
    def __init__(self, que, logger):
        self._que = que
        self._thread = Thread(target=self._run)

        self._env = Environment.get_instance()
        self._logger = logger

        self.running = True

    def _run(self):
        raise NotImplementedError()

    def _yield_event(self, event):
        self._que.push(event)

    def start(self):
        self._thread.start()


class TimerEventSource(SubEvnetSource):
    def __init__(self, que, logger, interval):
        super(TimerEventSource, self).__init__(que, logger)
        self._interval = interval

        self._before_trading_yielded = False
        self._after_trading_yielded = False
        self._settlement_yielded = False

    def _run(self):
        while self.running:
            sleep(self._interval)

            calendar_dt = self._env.get_instance().calendar_dt
            try:
                trading_dt = Environment.get_instance().data_proxy.get_trading_dt(calendar_dt)
            except RuntimeError:
                continue
            self._yield_event(Event(EVENT.DO_PERSIST, calendar_dt=calendar_dt, trading_dt=trading_dt))

            if not self._before_trading_yielded:
                if calendar_dt.hour >= 20:
                    self._yield_event(Event(EVENT.BEFORE_TRADING, calendar_dt=calendar_dt, trading_dt=trading_dt))
                    self._before_trading_yielded = True
            if self._before_trading_yielded:
                if calendar_dt.hour < 20:
                    self._before_trading_yielded = False

            if not self._after_trading_yielded:
                if calendar_dt.hour >= 16:
                    self._yield_event(Event(EVENT.AFTER_TRADING, calendar_dt=calendar_dt, trading_dt=trading_dt))
                    self._after_trading_yielded = True
            if self._after_trading_yielded:
                if calendar_dt.hour < 16:
                    self._after_trading_yielded = False

            if not self._settlement_yielded:
                if calendar_dt.hour >= 18:
                    self._yield_event(Event(EVENT.SETTLEMENT, calendar_dt=calendar_dt, trading_dt=trading_dt))
                    self._settlement_yielded = True
            if self._settlement_yielded:
                if calendar_dt.hour < 18:
                    self._settlement_yielded = False