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
from six import itervalues
from threading import Lock

from rqalpha.events import Event, EVENT
from rqalpha.model.trade import Trade
from rqalpha.utils.logger import system_log
from rqalpha.const import ORDER_STATUS, DEFAULT_ACCOUNT_TYPE, COMMISSION_TYPE, POSITION_EFFECT
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
            mod_config.trade_frontend_url,
            system_log
        )
        self._trade_api.on_order_status_updated = self.on_order_status_updated
        self._trade_api.on_order_cancel_failed = self.on_order_cancel_failed
        self._trade_api.on_trade = self.on_trade
        try:
            self._account = FutureAccount(
                env.config.base.accounts[DEFAULT_ACCOUNT_TYPE.FUTURE.name],Positions(FuturePosition)
            )
        except KeyError:
            self._account = FutureAccount(self._env.config.base.future_starting_cash, Positions(FuturePosition))

        self._open_orders = {}
        self._lock = Lock()

        self._trade_api.start_up()

    def submit_order(self, order):
        system_log.debug('TradeGateway: submit order {}'.format(order.order_id))
        try:
            self._trade_api.submit_order(order)
            self._open_orders[order.order_id] = order
        except RuntimeError as e:
            order.mark_rejected(e)
            self._que.put(Event(EVENT.ORDER_CREATION_REJECT, account=self._account, order=order))

    def cancel_order(self, order):
        system_log.info('TradeGateway: cancel order {}'.format(order.order_id))
        try:
            self._trade_api.cancel_order(order)
        except RuntimeError:
            if not order.is_final():
                order.active()
            self._que.put(Event(EVENT.ORDER_CANCELLATION_REJECT, account=self._account, order=copy(order)))

    def tear_down(self):
        self._trade_api.tear_down()

    @property
    def open_orders(self):
        with self._lock:
            return list(itervalues(self._open_orders))

    @property
    def future_infos(self):
        return self._trade_api.future_infos

    @property
    def account(self):
        return self._account

    def on_order_status_updated(self, order_id, status, message, secondary_order_id=None):
        with self._lock:
            try:
                order = self._open_orders[order_id]
            except KeyError:
                system_log.warn('Order status updated but no open_order was found, order_id: {}, status: {}, msgs: {}'.format(
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
                if order.status == ORDER_STATUS.PENDING_CANCEL:
                    order.mark_cancelled(message)
                    self._que.put(Event(EVENT.ORDER_CANCELLATION_PASS, account=self._account, order=order))
                elif order.status in [ORDER_STATUS.PENDING_NEW, ORDER_STATUS.ACTIVE]:
                    order.mark_cancelled(message)
                    self._que.put(Event(EVENT.ORDER_UNSOLICITED_UPDATE, account=self._account, order=order))
                del self._open_orders[order_id]
            elif status == ORDER_STATUS.REJECTED:
                if order.status == ORDER_STATUS.PENDING_NEW:
                    order.mark_rejected(message)
                    self._que.put(Event(EVENT.ORDER_CREATION_REJECT, account=self._account, order=order))
                elif order.status in [ORDER_STATUS.ACTIVE, ORDER_STATUS.PENDING_CANCEL]:
                    order.mark_cancelled(message)
                    self._que.put(Event(EVENT.ORDER_UNSOLICITED_UPDATE, account=self._account, order=order))
                del self._open_orders[order_id]

    def on_order_cancel_failed(self, order_id, reject_reason):
        with self._lock:
            try:
                order = self._open_orders[order_id]
            except KeyError:
                return
            if not order.is_final():
                order.active()
            system_log.info('order {} cancel rejected: {}'.format(order_id, reject_reason))
            self._que.put(Event(EVENT.ORDER_CANCELLATION_REJECT, account=self._account, order=copy(order)))

    def on_trade(self, order_id, exec_id, price, quantity, entry_time, calendar_date, trading_date, secondary_order_id=None):
        with self._lock:
            try:
                order = self._open_orders[order_id]
            except KeyError:
                system_log.warn(
                    'Trade happened but no open_order was found, gwy_order_id: {}, trade_id: {}'.format(order_id, exec_id)
                )
                return
            if secondary_order_id:
                order.set_secondary_order_id(secondary_order_id)
            # FIXME: 部分成交时收到重复的回报需丢弃
            if order.status == ORDER_STATUS.PENDING_NEW:
                order.active()
                self._que.put(Event(EVENT.ORDER_CREATION_PASS, account=self._account, order=copy(order)))
            trade = Trade.__from_create__(
                order_id=order_id,
                price=price,
                amount=quantity,
                side=order.side,
                position_effect=order.position_effect,
                order_book_id=order.order_book_id,
                commission=self.calc_commission(order.order_book_id, price, quantity, order.position_effect),
                tax=0,
                trade_id=exec_id,
                frozen_price=order.frozen_price,
            )
            order.fill(trade)
            if order.is_final():
                del self._open_orders[order_id]
            self._que.put(Event(EVENT.TRADE, account=self._account, trade=trade, order=copy(order)))

    def calc_commission(self, order_book_id, price, quantity, position_effect):
        info = self._env.data_proxy.get_commission_info(order_book_id)
        commission = 0
        if info['commission_type'] == COMMISSION_TYPE.BY_MONEY:
            contract_multiplier = self._env.get_instrument(order_book_id).contract_multiplier
            if position_effect == POSITION_EFFECT.OPEN:
                commission += price * quantity * contract_multiplier * info[
                    'open_commission_ratio']
            elif position_effect == POSITION_EFFECT.CLOSE:
                commission += price * quantity * contract_multiplier * info[
                    'close_commission_ratio']
            elif position_effect == POSITION_EFFECT.CLOSE_TODAY:
                commission += price * quantity * contract_multiplier * info[
                    'close_commission_today_ratio']
        else:
            if position_effect == POSITION_EFFECT.OPEN:
                commission += quantity * info['open_commission_ratio']
            elif position_effect == POSITION_EFFECT.CLOSE:
                commission += quantity * info['close_commission_ratio']
            elif position_effect == POSITION_EFFECT.CLOSE_TODAY:
                commission += quantity * info['close_commission_today_ratio']
        return commission
