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
from copy import copy
from six import itervalues
from threading import Lock

from rqalpha.events import Event, EVENT
from rqalpha.model.trade import Trade
from rqalpha.utils import id_gen
from rqalpha.utils.logger import system_log
from rqalpha.const import ORDER_STATUS, DEFAULT_ACCOUNT_TYPE, COMMISSION_TYPE, POSITION_EFFECT
from rqalpha.model.base_position import Positions

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
            system_log,
            mod_config.real_init_position
        )
        self._trade_api.on_order_status_updated = self.on_order_status_updated
        self._trade_api.on_order_cancel_failed = self.on_order_cancel_failed
        self._trade_api.on_trade = self.on_trade

        # order_id from rqalpha is too long
        self._order_ref_gen = id_gen(int(time.time()))
        self._order_ref_map = {}

        try:
            self._account = env.get_account_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name)(
                env.config.base.accounts[DEFAULT_ACCOUNT_TYPE.FUTURE.name],
                Positions(env.get_position_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name))
            )
        except KeyError:
            self._account = env.get_account_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name)(
                self._env.config.base.future_starting_cash,
                Positions(env.get_position_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name))
            )

        self._lock = Lock()

        self._trade_api.start_up()

        self._open_orders = copy(self._trade_api.open_orders)
        if mod_config.real_init_position:
            if env.config.base.init_positions:
                raise RuntimeError(
                    "RQAlpha receive init positions. rqalpha_mod_ctp do not support init_positions.")
            env.event_bus.add_listener(
                EVENT.POST_SYSTEM_INIT, lambda e: self._account.set_state(self._trade_api.account_state)
            )
            self.snapshot = {p['order_book_id']: {
                "order_book_id": p['order_book_id'],
                "last": p["prev_settlement_price"],
                "limit_up": p["prev_settlement_price"] * 1.1,
                "limit_down": p["prev_settlement_price"] * 0.9
            } for p in itervalues(self._trade_api.account_state['positions'])}
        else:
            self.snapshot = {}

    def submit_order(self, order):
        system_log.debug('TradeGateway: submit order {}'.format(order.order_id))
        order_ref = next(self._order_ref_gen)
        try:
            self._trade_api.submit_order(order_ref, order)
            self._cache_open_order(order_ref, order)
        except RuntimeError as e:
            order.mark_rejected(e)
            self._que.put(Event(EVENT.ORDER_CREATION_REJECT, account=self._account, order=order))

    def cancel_order(self, order):
        system_log.info('TradeGateway: cancel order {}'.format(order.order_id))
        try:
            order_ref = self._order_ref_map[order.order_id]
        except KeyError:
            self._que.put(Event(EVENT.ORDER_CANCELLATION_REJECT, account=self._account, order=copy(order)))
            return

        try:
            self._trade_api.cancel_order(order_ref, order)
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

    def on_order_status_updated(self, order_ref, status, message, secondary_order_id=None):
        try:
            order = self._get_open_order(order_ref)
        except KeyError:
            system_log.debug('Order status updated but no open_order was found, order_ref: {}, status: {}, msgs: {}'.format(
                order_ref, status, message
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
            self._del_open_order(order_ref)
        elif status == ORDER_STATUS.REJECTED:
            if order.status == ORDER_STATUS.PENDING_NEW:
                order.mark_rejected(message)
                self._que.put(Event(EVENT.ORDER_CREATION_REJECT, account=self._account, order=order))
            elif order.status in [ORDER_STATUS.ACTIVE, ORDER_STATUS.PENDING_CANCEL]:
                order.mark_cancelled(message)
                self._que.put(Event(EVENT.ORDER_UNSOLICITED_UPDATE, account=self._account, order=order))
            self._del_open_order(order_ref)

    def on_order_cancel_failed(self, order_ref, reject_reason):
        try:
            order = self._get_open_order(order_ref)
        except KeyError:
            return
        if not order.is_final():
            order.active()
        system_log.info('order {} cancel rejected: {}'.format(order.order_id, reject_reason))
        self._que.put(Event(EVENT.ORDER_CANCELLATION_REJECT, account=self._account, order=copy(order)))

    def on_trade(self, order_ref, exec_id, price, quantity, entry_time, calendar_date, trading_date, secondary_order_id=None):
        try:
            order = self._get_open_order(order_ref)
        except KeyError:
            system_log.warn(
                'Trade happened but no open_order was found, order_ref: {}, trade_id: {}'.format(order_ref, exec_id)
            )
            return
        if secondary_order_id:
            order.set_secondary_order_id(secondary_order_id)
        # FIXME: 部分成交时收到重复的回报需丢弃
        if order.status == ORDER_STATUS.PENDING_NEW:
            order.active()
            self._que.put(Event(EVENT.ORDER_CREATION_PASS, account=self._account, order=copy(order)))
        trade = Trade.__from_create__(
            order_id=order.order_id,
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
            self._del_open_order(order_ref)
        self._que.put(Event(EVENT.TRADE, account=self._account, trade=trade, order=copy(order)))

    def on_qry_account(self, account_state):
        self._account.set_state(account_state)

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

    def _cache_open_order(self, order_ref, order):
        with self._lock:
            self._open_orders[order_ref] = order
            self._order_ref_map[order.order_id] = order_ref

    def _get_open_order(self, order_ref):
        return self._open_orders[order_ref]

    def _del_open_order(self, order_ref):
        with self._lock:
            order = self._open_orders.pop(order_ref)
            del self._order_ref_map[order.order_id]
