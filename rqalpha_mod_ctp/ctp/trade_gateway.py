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
from six import iteritems, itervalues
from datetime import date

from rqalpha.utils.logger import system_log
from rqalpha.const import DEFAULT_ACCOUNT_TYPE, ORDER_STATUS,  SIDE, POSITION_EFFECT
from rqalpha.environment import Environment
from rqalpha.events import EVENT
from rqalpha.events import Event as RqEvent
from rqalpha.model.order import Order
from rqalpha.model.trade import Trade
from rqalpha.model.portfolio import Portfolio
from rqalpha.model.base_position import Positions

from .api import CtpTdApi
from .data_dict import FakeTickDict
from ..utils import cal_commission, margin_of


class TradeGateway(object):
    def __init__(self, env, retry_times=5, retry_interval=1):
        self._env = env

        self._retry_times = retry_times
        self._retry_interval = retry_interval

        self._cache = DataCache()

        self._query_returns = {}
        self._data_update_date = date.min

        self.td_api = None

        Environment.get_ins_dict = self.get_ins_dict

    def connect(self, user_id, password, broker_id, td_address):

        self.td_api = CtpTdApi(self, user_id, password, broker_id, td_address)
        for i in range(self._retry_times):
            self.td_api.connect()
            sleep(self._retry_interval * (i+1))
            if self.td_api.logged_in:
                self.on_log('CTP 交易服务器登录成功')
                break
        else:
            raise RuntimeError('CTP 交易服务器连接或登录超时')

        self.on_log('同步数据中。')

        if self._data_update_date != date.today():
            self._qry_instrument()
            self._qry_account()
            self._qry_position()
            self._qry_order()
            self._data_update_date = date.today()
            self._qry_commission()

        sleep(5)
        self.on_log('数据同步完成。')

    def submit_order(self, order):
        self.td_api.sendOrder(order)
        self._cache.cache_order(order)

    def cancel_order(self, order):
        account = Environment.get_instance().get_account(order.order_book_id)
        self._env.event_bus.publish_event(RqEvent(EVENT.ORDER_PENDING_CANCEL, account=account, order=order))
        self.td_api.cancelOrder(order)

    def get_portfolio(self):
        FuturePosition = self._env.get_position_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name)
        FutureAccount = self._env.get_account_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name)
        self._cache.set_models(FutureAccount, FuturePosition)
        future_account, static_value = self._cache.account
        start_date = self._env.config.base.start_date
        future_starting_cash = self._env.config.base.future_starting_cash
        accounts = {
            DEFAULT_ACCOUNT_TYPE.FUTURE.name: future_account
        }
        return Portfolio(start_date, static_value/future_starting_cash, future_starting_cash, accounts)

    def get_ins_dict(self, order_book_id=None):
        if order_book_id is not None:
            return self._cache.ins.get(order_book_id)
        else:
            return self._cache.ins

    def get_future_info(self, underlying_symbol):
        return self._cache.future_info.get(underlying_symbol)

    def exit(self):
        self.td_api.close()

    def on_query(self, api_name, n, result):
        self._query_returns[n] = result

    def on_order(self, order_dict):
        if not order_dict.is_valid:
            return
        self.on_debug('订单回报: %s' % str(order_dict))
        if self._data_update_date != date.today():
            return

        order = self._cache.get_cached_order(order_dict)

        account = Environment.get_instance().get_account(order.order_book_id)

        if order.status == ORDER_STATUS.PENDING_NEW:
            self._env.event_bus.publish_event(RqEvent(EVENT.ORDER_PENDING_NEW, account=account, order=order))
            order.active()
            self._env.event_bus.publish_event(RqEvent(EVENT.ORDER_CREATION_PASS, account=account, order=order))
            if order_dict.status == ORDER_STATUS.ACTIVE:
                self._cache.cache_open_order(order)
            elif order_dict.status in [ORDER_STATUS.CANCELLED, ORDER_STATUS.REJECTED]:
                order.mark_rejected('Order was rejected or cancelled.')
                self._env.event_bus.publish_event(RqEvent(EVENT.ORDER_UNSOLICITED_UPDATE, account=account, order=order))
                self._cache.remove_open_order(order)

            elif order_dict.status == ORDER_STATUS.FILLED:
                order._status = order_dict.status
                self._cache.remove_open_order(order)

        elif order.status == ORDER_STATUS.ACTIVE:
            if order_dict.status == ORDER_STATUS.FILLED:
                order._status = order_dict.status
            if order_dict.status == ORDER_STATUS.CANCELLED:
                order.mark_cancelled("%d order has been cancelled." % order.order_id)
                self._env.event_bus.publish_event(RqEvent(EVENT.ORDER_CANCELLATION_PASS, account=account, order=order))
                self._cache.remove_open_order(order)

        elif order.status == ORDER_STATUS.PENDING_CANCEL:
            if order_dict.status == ORDER_STATUS.CANCELLED:
                order.mark_cancelled("%d order has been cancelled." % order.order_id)
                self._env.event_bus.publish_event(RqEvent(EVENT.ORDER_CANCELLATION_PASS, account=account, order=order))
                self._cache.remove_open_order(order)
            if order_dict.status == ORDER_STATUS.FILLED:
                order._status = order_dict.status
                self._cache.remove_open_order(order)

    def on_trade(self, trade_dict):
        self.on_debug('交易回报: %s' % str(trade_dict))
        if self._data_update_date != date.today():
            self._cache.cache_trade(trade_dict)
        else:
            account = Environment.get_instance().get_account(trade_dict.order_book_id)

            if trade_dict.trade_id in account._backward_trade_set:
                return

            order = self._cache.get_cached_order(trade_dict)
            commission = cal_commission(trade_dict, order.position_effect)
            trade = Trade.__from_create__(
                trade_dict.order_id, trade_dict.price, trade_dict.amount,
                trade_dict.side, trade_dict.position_effect, trade_dict.order_book_id, trade_id=trade_dict.trade_id,
                commission=commission, frozen_price=trade_dict.price)

            order.fill(trade)
            self._env.event_bus.publish_event(RqEvent(EVENT.TRADE, account=account, trade=trade))

    def _qry_instrument(self):
        for i in range(self._retry_times):
            req_id = self.td_api.qryInstrument()
            sleep(self._retry_interval * (i+1))
            if req_id in self._query_returns:
                ins_cache = self._query_returns[req_id].copy()
                del self._query_returns[req_id]
                self.on_debug('%d 条合约数据返回。' % len(ins_cache))
                self._cache.cache_ins(ins_cache)
                break
        else:
            raise RuntimeError('请求合约数据超时')

    def _qry_account(self):
        for i in range(self._retry_times):
            req_id = self.td_api.qryAccount()
            sleep(self._retry_interval * (i+1))
            if req_id in self._query_returns:
                account_dict = self._query_returns[req_id].copy()
                del self._query_returns[req_id]
                self.on_debug('账户数据返回: %s' % str(account_dict))
                self._cache.cache_account(account_dict)
                break
        else:
            raise RuntimeError('请求账户数据超时')

    def _qry_position(self):
        for i in range(self._retry_times):
            req_id = self.td_api.qryPosition()
            sleep(self._retry_interval * (i+1))
            if req_id in self._query_returns:
                positions = self._query_returns[req_id].copy()
                del self._query_returns[req_id]
                self.on_debug('持仓数据返回: %s。' % str(positions.keys()))
                self._cache.cache_position(positions)
                break

    def _qry_order(self):
        for i in range(self._retry_times):
            req_id = self.td_api.qryOrder()
            sleep(self._retry_interval * (i+1))
            if req_id in self._query_returns:
                order_cache = self._query_returns[req_id].copy()
                del self._query_returns[req_id]
                self.on_debug('订单数据返回')
                for order_dict in order_cache.values():
                    order = self._cache.get_cached_order(order_dict)
                    if order_dict.status == ORDER_STATUS.ACTIVE:
                        self._cache.cache_open_order(order)
                self._cache.cache_qry_order(order_cache)

                break

    def _qry_commission(self):
        for order_book_id, ins_dict in iteritems(self._cache.ins):
            if ins_dict.underlying_symbol in self._cache.future_info and 'commission_type' in self._cache.future_info[ins_dict.underlying_symbol]['speculation']:
                continue
            for i in range(self._retry_times):
                req_id = self.td_api.qryCommission(order_book_id)
                sleep(self._retry_interval * (i + 1))
                if req_id in self._query_returns:
                    commission_dict = self._query_returns[req_id].copy()
                    del self._query_returns[req_id]
                    self._cache.cache_commission(ins_dict.underlying_symbol, commission_dict)
                    break
        self.on_debug('费率数据返回')

    @property
    def open_orders(self):
        return self._cache.open_orders

    @property
    def snapshot(self):
        return self._cache.snapshot

    @staticmethod
    def on_debug(debug):
        system_log.debug(debug)

    @staticmethod
    def on_log(log):
        system_log.info(log)

    @staticmethod
    def on_err(error, func_name):
        system_log.error('CTP 错误，错误代码：%s，错误信息：%s' % (str(error.ErrorID), error.ErrorMsg.decode('GBK')))


class DataCache(object):
    def __init__(self):
        self.ins = {}
        self.future_info = {}

        self.orders = {}
        self.open_orders = []
        self.trades = {}

        self.pos = {}
        self.snapshot = {}

        self._account_dict = None
        self._qry_order_cache = {}
        self._account_model = None
        self._position_model = None

    def cache_ins(self, ins_cache):
        self.ins = ins_cache
        self.future_info = {ins_dict.underlying_symbol: {'speculation': {
                'long_margin_ratio': ins_dict.long_margin_ratio,
                'short_margin_ratio': ins_dict.short_margin_ratio,
                'margin_type': ins_dict.margin_type,
            }} for ins_dict in self.ins.values()}

    def cache_commission(self, underlying_symbol, commission_dict):
        self.future_info[underlying_symbol]['speculation'].update({
            'open_commission_ratio': commission_dict.open_ratio,
            'close_commission_ratio': commission_dict.close_ratio,
            'close_commission_today_ratio': commission_dict.close_today_ratio,
            'commission_type': commission_dict.commission_type,
        })

    def cache_open_order(self, order):
        if order not in self.open_orders:
            self.open_orders.append(order)

    def remove_open_order(self, order):
        if order in self.open_orders:
            self.open_orders.remove(order)

    def cache_position(self, pos_cache):
        self.pos = pos_cache
        for order_book_id, pos_dict in iteritems(pos_cache):
            if order_book_id not in self.snapshot:
                self.snapshot[order_book_id] = FakeTickDict(pos_dict)

    def cache_account(self, account_dict):
        self._account_dict = account_dict

    def cache_qry_order(self, order_cache):
        self._qry_order_cache = order_cache

    def cache_trade(self, trade_dict):
        if trade_dict.order_book_id not in self.trades:
            self.trades[trade_dict.order_book_id] = []
        self.trades[trade_dict.order_book_id].append(trade_dict)

    def get_cached_order(self, obj):
        try:
            order = self.orders[obj.order_id]
        except KeyError:
            order = Order.__from_create__(obj.order_book_id, obj.quantity, obj.side, obj.style, obj.position_effect)
            self.cache_order(order)
        return order

    def cache_order(self, order):
        self.orders[order.order_id] = order

    @property
    def positions(self):
        PositionModel = self._position_model
        ps = Positions(PositionModel)
        for order_book_id, pos_dict in iteritems(self.pos):
            position = PositionModel(order_book_id)

            position._buy_old_holding_list = [(pos_dict.prev_settle_price, pos_dict.buy_old_quantity)]
            position._sell_old_holding_list = [(pos_dict.prev_settle_price, pos_dict.sell_old_quantity)]

            position._buy_transaction_cost = pos_dict.buy_transaction_cost
            position._sell_transaction_cost = pos_dict.sell_transaction_cost
            position._buy_realized_pnl = pos_dict.buy_realized_pnl
            position._sell_realized_pnl = pos_dict.sell_realized_pnl

            position._buy_avg_open_price = pos_dict.buy_avg_open_price
            position._sell_avg_open_price = pos_dict.sell_avg_open_price

            if order_book_id in self.trades:
                trades = sorted(self.trades[order_book_id], key=lambda t: t.trade_id, reverse=True)

                buy_today_holding_list = []
                sell_today_holding_list = []
                for trade_dict in trades:
                    if trade_dict.side == SIDE.BUY and trade_dict.position_effect == POSITION_EFFECT.OPEN:
                        buy_today_holding_list.append((trade_dict.price, trade_dict.quantity))
                    elif trade_dict.side == SIDE.SELL and trade_dict.position_effect == POSITION_EFFECT.OPEN:
                        sell_today_holding_list.append((trade_dict.price, trade_dict.quantity))

                self.process_today_holding_list(pos_dict.buy_today_quantity, buy_today_holding_list)
                self.process_today_holding_list(pos_dict.sell_today_quantity, sell_today_holding_list)

                position._buy_today_holding_list = buy_today_holding_list
                position._sell_today_holding_list = sell_today_holding_list

            ps[order_book_id] = position
        return ps

    def process_today_holding_list(self, today_quantity, holding_list):
        # check if list is empty
        if not holding_list:
            return
        cum_quantity = sum(quantity for price, quantity in holding_list)
        left_quantity = cum_quantity - today_quantity
        while left_quantity > 0:
            oldest_price, oldest_quantity = holding_list.pop()
            if oldest_quantity > left_quantity:
                consumed_quantity = left_quantity
                holding_list.append((oldest_price, oldest_quantity - left_quantity))
            else:
                consumed_quantity = oldest_quantity
            left_quantity -= consumed_quantity

    @property
    def account(self):
        static_value = self._account_dict.yesterday_portfolio_value
        ps = self.positions
        realized_pnl = sum(position.realized_pnl for position in itervalues(ps))
        cost = sum(position.transaction_cost for position in itervalues(ps))
        margin = sum(position.margin for position in itervalues(ps))
        total_cash = static_value + realized_pnl - cost - margin

        AccountModel = self._account_model
        account = AccountModel(total_cash, ps)
        account._frozen_cash = sum(
            [margin_of(order_dict.order_book_id, order_dict.unfilled_quantity, order_dict.price) for order_dict in
             self._qry_order_cache.values() if order_dict.status == ORDER_STATUS.ACTIVE])
        return account, static_value

    def set_models(self, account_model, position_model):
        self._account_model = account_model
        self._position_model = position_model
