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

from queue import Queue, Empty

from rqalpha.events import EVENT, Event
from rqalpha.interface import AbstractEventSource
from rqalpha.utils import rq_json as json_utils
from rqalpha.utils import get_trading_period, is_night_trading, is_trading

from .sub_event_source import EVENT_DO_SEND_FEED


class QueuedEventSource(AbstractEventSource):
    def __init__(self, env, logger):
        self._env = env
        self._logger = logger
        self._queue = Queue()

        self._last_before_trading = None
        self._need_night_trading = False
        self._universe_changed = False

        self._env.event_bus.add_listener(EVENT.POST_UNIVERSE_CHANGED, self._on_universe_change)

    def set_state(self, state):
        persist_dict = json_utils.convert_json_to_dict(state.decode('utf-8'))
        self._last_before_trading = persist_dict['_last_before_trading']
        self._need_night_trading = persist_dict['_need_night_trading']

    def get_state(self):
        return json_utils.convert_dict_to_json({
            "_last_before_trading": self._last_before_trading,
            "_need_night_trading": self._need_night_trading,
        }).encode('utf-8')

    def _on_universe_change(self, *args):
        self._universe_changed = True

    def push(self, event):
        self._queue.put(event)

    MARKET_DATA_EVENTS = {EVENT.TICK, EVENT.BAR, EVENT.BEFORE_TRADING, EVENT.AFTER_TRADING, EVENT.SETTLEMENT}

    @staticmethod
    def _filter_events(events):
        if len(events) == 1:
            return events

        last_bar_pos = None
        last_tick_pos = None
        last_persist_event_pos = None
        last_send_feed_event_pos = None
        for i, e in enumerate(events):
            if e.event_type == EVENT.BAR:
                last_bar_pos = i
            elif e.event_type == EVENT.TICK:
                last_tick_pos = i
            if e.event_type == EVENT.DO_PERSIST:
                last_persist_event_pos = i
            if e.event_type == EVENT_DO_SEND_FEED:
                last_send_feed_event_pos = i
        results = []
        for i, e in enumerate(events):
            if e.event_type == EVENT.BAR:
                if i == last_bar_pos:
                    results.append(e)
            elif e.event_type == EVENT.TICK:
                if i == last_tick_pos:
                    results.append(e)
            elif e.event_type == EVENT.DO_PERSIST:
                if i == last_persist_event_pos:
                    results.append(e)
            elif e.event_type == EVENT_DO_SEND_FEED:
                if i == last_send_feed_event_pos:
                    results.append(e)
            else:
                results.append(e)
        return results

    def events(self, start_date, end_date, frequency):
        if frequency != 'tick':
            raise NotImplementedError()

        trading_periods = get_trading_period(self._env.get_universe(), self._env.config.base.accounts)
        self._need_night_trading = is_night_trading(self._env.get_universe())

        events = []

        config = self._env.config
        force_run_before_trading = config.extra.force_run_init_when_pt_resume

        while True:
            if self._universe_changed:
                trading_periods = get_trading_period(self._env.get_universe(), self._env.config.base.accounts)
                self._need_night_trading = is_night_trading(self._env.get_universe())

            while True:
                try:
                    events.append(self._queue.get_nowait())
                except Empty:
                    break

            filtered = self._filter_events(events)

            for e in filtered:
                if not self._need_night_trading and e.event_type in self.MARKET_DATA_EVENTS and \
                        (e.calendar_dt.hour > 19 or e.calendar_dt.hour < 4):
                    continue
                if e.event_type == EVENT.TICK:
                    # tick 是最频繁的
                    if not is_trading(e.trading_dt, trading_periods):
                        continue
                    if self._last_before_trading != e.trading_dt.date() or force_run_before_trading:
                        force_run_before_trading = False
                        self._last_before_trading = e.trading_dt.date()
                        self._logger.debug('EVNET: {}'.format(str(Event(EVENT.BEFORE_TRADING, calendar_dt=e.calendar_dt, trading_dt=e.trading_dt))))
                        yield Event(EVENT.BEFORE_TRADING, calendar_dt=e.calendar_dt, trading_dt=e.trading_dt)
                    else:
                        self._logger.debug('EVNET: {}'.format(str(e)))
                        yield e

                elif e.event_type == EVENT.BEFORE_TRADING:
                    if self._last_before_trading != e.trading_dt.date():
                        force_run_before_trading = False
                        self._last_before_trading = e.trading_dt.date()
                        self._logger.debug('EVNET: {}'.format(str(e)))
                        yield e
                else:
                    self._logger.debug('EVNET: {}'.format(str(e)))
                    yield e

            events.clear()
            events.append(self._queue.get())
