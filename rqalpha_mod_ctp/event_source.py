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

from time import sleep

try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty

from threading import Thread
from six import iteritems

from rqalpha.environment import Environment
from rqalpha.events import EVENT, Event
from rqalpha.interface import AbstractEventSource
from rqalpha.utils import rq_json as json_utils
from rqalpha.utils import is_night_trading
from rqalpha.utils.logger import system_log


class QueuedEventSource(AbstractEventSource):
    def __init__(self, env):
        self._env = env
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

    def put(self, event):
        self._queue.put(event)

    MARKET_DATA_EVENTS = {EVENT.TICK, EVENT.BEFORE_TRADING, EVENT.AFTER_TRADING, EVENT.SETTLEMENT}

    @staticmethod
    def _filter_events(events):
        if len(events) == 1:
            return events

        seen = set()
        results = []
        for e in events[::-1]:
            if e.event_type == EVENT.TICK:
                if EVENT.TICK not in seen:
                    results.append(e)
                    seen.add(EVENT.TICK)
            elif e.event_type == EVENT.DO_PERSIST:
                if EVENT.DO_PERSIST not in seen:
                    results.append(e)
                    seen.add(EVENT.DO_PERSIST)
            else:
                results.append(e)
        return results[::-1]

    def events(self, start_date, end_date, frequency):
        if frequency != 'tick':
            raise NotImplementedError()

        self._need_night_trading = is_night_trading(self._env.get_universe())

        events = []

        config = self._env.config
        force_run_before_trading = config.extra.force_run_init_when_pt_resume

        while True:
            if self._universe_changed:
                self._universe_changed = False
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
                system_log.debug('got event {}', e)
                if e.event_type == EVENT.TICK:
                    if self._last_before_trading != e.trading_dt.date() or force_run_before_trading:
                        force_run_before_trading = False
                        self._last_before_trading = e.trading_dt.date()
                        yield Event(EVENT.BEFORE_TRADING, calendar_dt=e.calendar_dt, trading_dt=e.trading_dt)
                    else:
                        yield e
                elif e.event_type == EVENT.BEFORE_TRADING:
                    if self._last_before_trading != e.trading_dt.date():
                        force_run_before_trading = False
                        self._last_before_trading = e.trading_dt.date()
                        system_log.debug('EVNET: {}'.format(str(e)))
                        yield e
                else:
                    system_log.debug('EVNET: {}'.format(str(e)))
                    yield e

            events.clear()
            events.append(self._queue.get())


class SubEvnetSource(object):
    def __init__(self, que):
        self._que = que
        self._thread = Thread(target=self._run)

        self._env = Environment.get_instance()

        self.running = True

    def _run(self):
        raise NotImplementedError()

    def _yield_event(self, event):
        self._que.put(event)

    def start(self):
        self._thread.start()

    def stop(self):
        self.running = False
        if self._thread.is_alive():
            self._thread.join()


class TimerEventSource(SubEvnetSource):
    def __init__(self, que, interval=1):
        super(TimerEventSource, self).__init__(que)
        self._interval = interval

        self._last_strategy_holding_status = False

    def _run(self):
        # todo: before_trading, after_trading, settlement
        while self.running:
            sleep(self._interval)
            calendar_dt = self._env.calendar_dt
            try:
                trading_dt = self._env.data_proxy.get_trading_dt(calendar_dt)
            except RuntimeError:
                continue

            current_strategy_holding_status = self._env.config.extra.is_hold

            if self._last_strategy_holding_status != current_strategy_holding_status:
                if not self._last_strategy_holding_status and current_strategy_holding_status:
                    self._yield_event(Event(EVENT.STRATEGY_HOLD_SET, calendar_dt=calendar_dt, trading_dt=trading_dt))
                elif self._last_strategy_holding_status and not current_strategy_holding_status:
                    self._yield_event(Event(EVENT.STRATEGY_HOLD_CANCELLED, calendar_dt=calendar_dt, trading_dt=trading_dt))
                self._last_strategy_holding_status = current_strategy_holding_status

            self._yield_event(Event(EVENT.DO_PERSIST, calendar_dt=calendar_dt, trading_dt=trading_dt))
