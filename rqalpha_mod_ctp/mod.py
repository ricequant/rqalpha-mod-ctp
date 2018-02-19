#!/usr/bin/env python
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

from datetime import date

from rqalpha.interface import AbstractMod
from rqalpha.const import RUN_TYPE, DEFAULT_ACCOUNT_TYPE
from rqalpha.utils.logger import system_log
from rqalpha.events import EVENT

from .ctp_broker import CtpBroker
from .ctp_data_source import CtpDataSource
from .ctp_price_board import CtpPriceBoard
from .ctp.md_gateway import MdGateway
from .ctp.trade_gateway import TradeGateway
from .event_source import QueuedEventSource, TimerEventSource


class CtpMod(AbstractMod):
    def __init__(self):
        self._env = None
        self._mod_config = None

        self._md_gateway = None
        self._trade_gateway = None

        self._event_source = None
        self._sub_event_sources = []

    def start_up(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config

        if not (mod_config.user_id and mod_config.password and mod_config.broker_id and mod_config.md_frontend_url and mod_config.trade_frontend_url):
            system_log.warn('Parameters are not complete, rqalpha-mod-ctp won\'t start.')
            return
        if env.config.base.run_type != RUN_TYPE.LIVE_TRADING and env.config.base.frequency != 'tick':
            system_log.warn('Run type or frequency not available, rqalpha-mod-ctp won\'t start.')
            return
        if DEFAULT_ACCOUNT_TYPE.FUTURE.name not in self._env.config.base.accounts:
            system_log.warn('Only support future, which is not in accounts, rqalpha-mod-ctp won\'t start.')
            return

        env.config.base.start_date = date.today()
        self._event_source = QueuedEventSource(env)

        self._trade_gateway = TradeGateway(env, mod_config, self._event_source)
        self._env.set_data_source(CtpDataSource(env, self._md_gateway, self._trade_gateway))

        self._md_gateway = MdGateway(env, mod_config, self._event_source)

        self._sub_event_sources.append(TimerEventSource(self._event_source))
        env.set_event_source(self._event_source)

        self._env.set_broker(CtpBroker(env, self._trade_gateway))

        self._env.set_price_board(CtpPriceBoard(self._md_gateway, self._trade_gateway))

        self._env.event_bus.add_listener(EVENT.POST_SYSTEM_INIT, self._run_sub_event_sources)

    def tear_down(self, code, exception=None):
        if self._md_gateway is not None:
            self._md_gateway.tear_down()
        if self._trade_gateway is not None:
            self._trade_gateway.tear_down()

    def _run_sub_event_sources(self, *args, **kwargs):
        for sub_event_source in self._sub_event_sources:
            sub_event_source.start()

