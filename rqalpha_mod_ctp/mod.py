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
        self._md_gateway = None
        self._trade_gateway = None

    def start_up(self, env, mod_config):
        self._env = env

        self._md_gateway = MdGateway(self._env)
        self._trade_gateway = TradeGateway(self._env)

        self._trade_gateway.connect(mod_config.CTP.userID, mod_config.CTP.password, mod_config.CTP.brokerID, mod_config.CTP.tdAddress)
        self._md_gateway.connect(mod_config.CTP.userID, mod_config.CTP.password, mod_config.CTP.brokerID, mod_config.CTP.mdAddress)

        self._env.set_broker(CtpBroker(self._trade_gateway))
        self._env.set_event_source(CtpEventSource(env, mod_config, self._md_gateway))
        self._env.set_data_source(CtpDataSource(env, self._md_gateway, self._trade_gateway))
        self._env.set_price_board(CtpPriceBoard(self._md_gateway, self._trade_gateway))

    def tear_down(self, code, exception=None):
        self._md_gateway.exit()
        self._trade_gateway.exit()
