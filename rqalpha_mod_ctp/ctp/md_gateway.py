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

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty
from six import itervalues

from rqalpha.utils.logger import system_log
from rqalpha.events import EVENT, Event
from rqalpha.model.tick import Tick

from .api import CtpMdApi


class MdGateway(object):
    def __init__(self, env, mod_config, que):
        self._env = env
        self._que = que
        self._md_api = CtpMdApi(
            mod_config.user_id,
            mod_config.password,
            mod_config.broker_id,
            mod_config.md_frontend_url,
            system_log
        )
        self._md_api.on_tick = self.on_tick
        self._md_api.get_instrument_ids = lambda: [
            info['instrument_id'] for info in itervalues(env.data_source.get_future_info())
        ]

        self._subscribed = None
        self._snapshot_cache = {}
        env.event_bus.add_listener(EVENT.POST_UNIVERSE_CHANGED, self.on_universe_changed)

        self._md_api.start_up()
        # TODO: 订阅

    def tear_down(self):
        self._md_api.tear_down()

    @property
    def snapshot(self):
        return self._snapshot_cache

    def on_tick(self, tick_dict):
        if not self._subscribed:
            return
        if tick_dict['order_book_id'] in self._subscribed:
            tick = Tick(tick_dict['order_book_id'], tick_dict)
            self._que.put(Event(
                EVENT.TICK,
                calendar_dt=tick.datetime,
                trading_dt=self._env.data_proxy.get_trading_dt(tick.datetime),
                tick=tick
            ))
        self._snapshot_cache[tick_dict['order_book_id']] = tick_dict

    def on_universe_changed(self, event):
        self._subscribed = event.universe
