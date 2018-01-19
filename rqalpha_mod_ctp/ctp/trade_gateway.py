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

from copy import copy
from threading import Lock

from rqalpha.utils.logger import system_log
from rqalpha.const import ORDER_STATUS, DEFAULT_ACCOUNT_TYPE
from rqalpha.model.base_position import Positions
from rqalpha.mod.rqalpha_mod_sys_accounts.account_model.future_account import FutureAccount
from rqalpha.mod.rqalpha_mod_sys_accounts.position_model.future_position import FuturePosition

from .api import CtpTradeApi


class TradeGateway(object):
    def __init__(self, env, mod_config, que):
        self._env = env
        self._que = que
        self._trade_api = CtpTradeApi(
            mod_config.user_id,
            mod_config.password,
            mod_config.broker_id,
            mod_config.md_frontend_url,
            system_log
        )
        self._trade_api.on_order_status_updated = self.on_order_status_updated
        self._trade_api.on_order_cancel_failed = self.on_order_cancel_failed
        self._trade_api.on_trade = self.on_trade

        self._account = FutureAccount(
            env.config.base.accounts.get(DEFAULT_ACCOUNT_TYPE.FUTURE.name, env.config.base.future_starting_cash),
            Positions(FuturePosition)
        )

        self._open_orders = {}
        self._orders_waiting_for_trade = {}
        self._lock = Lock()

        self._trade_api.start_up()

    def submit_order(self, order):
        system_log.debug('TradeGateway: submit order {}'.format(order.order_id))
        self._trade_api.submit_order(order)

    def cancel_order(self, order):
        system_log.info('TradeGateway: cancel order {}'.format(order.order_id))
        self._trade_api.cancel_order(order)

    def tear_down(self):
        self._trade_api.tear_down()

    def on_order_status_updated(self, order_id, status, message, secondary_order_id=None):
        with self._lock:
            try:
                order = self._open_orders[order_id]
            except KeyError:
                system_log.error('Order status updated but no open_order was found, order_id: {}, status: {}, msgs: {}'.format(
                        order_id, status, message
                ))
                return
            if secondary_order_id:
                order.set_secondary_order_id(secondary_order_id)
            if status == ORDER_STATUS.ACTIVE:
                if order.status == ORDER_STATUS.PENDING_NEW:
                    order.active()
                    self._que.put(Event(EVENT.ORDER_CREATION_PASS, account=self._account, order=copy(order)))

            elif status == ORDER_STATUS.CANCELLED:
                pass
            elif status == ORDER_STATUS.REJECTED:
                pass

    def on_order_cancel_failed(self, order_id, reject_reason):
        pass

    def on_trade(self, order_id, exec_id, price, quantity, entry_time, calendar_date, trading_date, secondary_order_id=None):
        pass
