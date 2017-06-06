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

from rqalpha.interface import AbstractMod
from .ctp_event_source import CtpEventSource
from .ctp_broker import CtpBroker
from .ctp_data_source import CtpDataSource
from .ctp_price_board import CtpPriceBoard

from .ctp.md_gateway import MdGateway
from .ctp.trade_gateway import TradeGateway


class CtpMod(AbstractMod):
    def __init__(self):
        self._env = None
        self._mod_config = None
        self._md_gateway = None
        self._trade_gateway = None

    def start_up(self, env, mod_config):
        self._env = env
        self._mod_config = mod_config

        self._init_trade_gateway()
        self._init_md_gateway()

        if mod_config.trade.enabled:
            self._env.set_broker(CtpBroker(env, self._trade_gateway))

        if mod_config.event.enabled:
            self._env.set_event_source(CtpEventSource(env, mod_config, self._md_gateway))
            self._env.set_data_source(CtpDataSource(env, self._md_gateway, self._trade_gateway))
            self._env.set_price_board(CtpPriceBoard(self._md_gateway, self._trade_gateway))

    def tear_down(self, code, exception=None):
        if self._md_gateway is not None:
            self._md_gateway.exit()
        if self._trade_gateway is not None:
            self._trade_gateway.exit()

    def _init_trade_gateway(self):
        if not self._mod_config.trade.enabled:
            return
        user_id = self._mod_config.login.user_id
        password = self._mod_config.login.password
        broker_id = self._mod_config.login.broker_id
        trade_frontend_uri = self._mod_config.trade.address

        self._trade_gateway = TradeGateway(self._env)
        self._trade_gateway.connect(user_id, password, broker_id, trade_frontend_uri)

    def _init_md_gateway(self):
        if not self._mod_config.event.enabled:
            return
        user_id = self._mod_config.login.user_id
        password = self._mod_config.login.password
        broker_id = self._mod_config.login.broker_id
        md_frontend_uri = self._mod_config.event.address

        self._md_gateway = MdGateway(self._env)
        self._md_gateway.connect(user_id, password, broker_id, md_frontend_uri)

