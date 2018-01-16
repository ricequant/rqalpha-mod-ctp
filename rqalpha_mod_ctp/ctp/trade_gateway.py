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
from copy import copy
from six import iteritems, itervalues

from rqalpha.utils.logger import system_log, user_system_log
from rqalpha.const import DEFAULT_ACCOUNT_TYPE, ORDER_STATUS,  SIDE, POSITION_EFFECT
from rqalpha.environment import Environment
from rqalpha.events import EVENT
from rqalpha.events import Event as RqEvent
from rqalpha.model.order import Order
from rqalpha.model.trade import Trade
from rqalpha.model.portfolio import Portfolio
from rqalpha.model.base_position import Positions

from .api import CtpTdApi
from ..utils import cal_commission, margin_of


class TradeGateway(object):
    def __init__(self, env, que, retry_times=5, retry_interval=2):
        self._env = env
        self._run_id = env.config.base.run_id
        self._que = que

        self._retry_times = retry_times
        self._retry_interval = retry_interval

        self._cache = DataCache()

        self._query_returns = {}

        self._open_orders = {}
        self._orders_waiting_for_trade = {}

        self._inited = False

        self.td_api = None

        Environment.get_ins_dict = self.get_ins_dict

    def connect(self, user_id, password, broker_id, td_address):
        self._inited = False
        self.td_api = CtpTdApi(self, user_id, password, broker_id, td_address)

        def wait_and_retry():
            for j in range(300):
                sleep(0.01)
                if self.td_api.logged_in:
                    return True
                elif self.td_api.err_id:
                    """
                    错误代码：
                        3: 不合法的登陆
                        75: 连续登陆次数超限，登录被禁止
                        7：CTP还未初始化
                    """
                    error = 'CTP 错误，错误代码：{}，错误信息：{}'.format(self.td_api.err_id, self.td_api.err_msg)
                    if self.td_api.err_id == 7:
                        user_system_log.error(error)
                    else:
                        raise RuntimeError(error)
            else:
                return False

        i = 1
        while True:
            i += 1
            self.td_api.connect()
            if wait_and_retry():
                system_log.info('Ctp trade server connected.')
                break

            if i < 10:
                sleep_time = 2 ** i
                user_system_log.error(
                    'Ctp trade server connect or login timeout, retry after {} seconds'.format(sleep_time))
            else:
                sleep_time = 900
                user_system_log.error('Ctp trad server connect or login timeout, retry after 15 minutes')
            sleep(sleep_time)

        system_log.info('同步数据中。')

        self._qry_instrument()
        self._qry_account()
        self._qry_position()
        self._qry_order()
        sleep(5)
        self._inited = True
        system_log.info('数据同步完成。')

    def submit_order(self, order):
        system_log.info('CTPTradeGateway: submit order {} by {}'.format(order.order_id, self._run_id))
        account = self._env.get_account(order.order_book_id)
        if not self._inited:
            order.mark_rejected('Ctp has not inited yet.')
            self._que.push(RqEvent(EVENT.ORDER_UNSOLICITED_UPDATE, account=account, order=copy(order)))
            return
        # self._cache.cache_open_order(order)
        self._que.push(RqEvent(EVENT.ORDER_PENDING_NEW, account=account, order=copy(order)))
        self._open_orders[order.order_id] = order
        self.td_api.sendOrder(order)
        # self._cache.cache_order(order)

    def cancel_order(self, order):
        system_log.info('CTPTradeGateway: cancel order {} by {}'.format(order.order_id, self._run_id))
        account = Environment.get_instance().get_account(order.order_book_id)
        self._que.push(RqEvent(EVENT.ORDER_PENDING_CANCEL, account=account, order=copy(order)))
        self.td_api.cancelOrder(order)

    def get_portfolio(self):
        FuturePosition = self._env.get_position_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name)
        FutureAccount = self._env.get_account_model(DEFAULT_ACCOUNT_TYPE.FUTURE.name)
        FutureAccount.AGGRESSIVE_UPDATE_LAST_PRICE = True
        self._cache.set_models(FutureAccount, FuturePosition)
        future_account, static_value = self._cache.account
        start_date = self._env.config.base.start_date
        future_starting_cash = self._env.config.base.accounts.get(DEFAULT_ACCOUNT_TYPE.FUTURE.name, 0)
        accounts = {
            DEFAULT_ACCOUNT_TYPE.FUTURE.name: future_account
        }
        return Portfolio(start_date, static_value/future_starting_cash, future_starting_cash, accounts)

    def get_ins_dict(self, order_book_id=None):
        if order_book_id is not None:
            return self._cache.ins.get(order_book_id)
        else:
            return self._cache.ins

    def exit(self):
        self.td_api.close()

    def on_query(self, api_name, n, result):
        self._query_returns[n] = result

    def on_order(self, order_dict):
        system_log.info('CTPTradeGateway: on order {} by {}'.format(order_dict.order_id, self._run_id))
        if not order_dict.is_valid:
            return
        self.on_debug('订单回报: %s' % str(order_dict))
        if not self._inited:
            system_log.warn(
                'Order {}, which was submitted before api inited, will be ignored. '.format(
                    str(order_dict.order_id)
                )
            )
            return

        order = self._open_orders.get(order_dict.order_id)
        # order = self._cache.get_cached_order(order_dict)
        if order is None:
            user_system_log.warn(
                'Order {}, which was not submitted by current strategy, will be ignored. '
                'As a result, account and position data in strategy may be inconsistent with CTP.'.format(
                    str(order_dict.order_id)
                )
            )
            return
        order._message = order_dict.message

        account = self._env.get_account(order.order_book_id)

        if order.status == ORDER_STATUS.PENDING_NEW:
            order.active()
            self._que.push(RqEvent(EVENT.ORDER_CREATION_PASS, account=account, order=copy(order)))
            if order_dict.status == ORDER_STATUS.ACTIVE:
                pass
            if order_dict.status in [ORDER_STATUS.CANCELLED, ORDER_STATUS.REJECTED]:
                order.mark_rejected(order_dict.message)
                self._que.push(RqEvent(EVENT.ORDER_UNSOLICITED_UPDATE, account=account, order=copy(order)))
                del self._open_orders[order_dict.order_id]
                # self._cache.remove_open_order(order)

            elif order_dict.status == ORDER_STATUS.FILLED:
                order._status = order_dict.status
                del self._open_orders[order_dict.order_id]
                self._orders_waiting_for_trade[order_dict.order_id] = order
                # self._cache.remove_open_order(order)

        elif order.status == ORDER_STATUS.ACTIVE:
            if order_dict.status == ORDER_STATUS.FILLED:
                del self._open_orders[order_dict.order_id]
                self._orders_waiting_for_trade[order_dict.order_id] = order
                order._status = order_dict.status

            if order_dict.status == ORDER_STATUS.CANCELLED:
                order.mark_cancelled("%d order has been cancelled." % order.order_id)
                self._que.push(RqEvent(EVENT.ORDER_CANCELLATION_PASS, account=account, order=copy(order)))
                del self._open_orders[order_dict.order_id]
                # self._cache.remove_open_order(order)

        elif order.status == ORDER_STATUS.PENDING_CANCEL:
            if order_dict.status == ORDER_STATUS.CANCELLED:
                order.mark_cancelled("%d order has been cancelled." % order.order_id)
                self._que.push(RqEvent(EVENT.ORDER_CANCELLATION_PASS, account=account, order=copy(order)))
                del self._open_orders[order_dict.order_id]
                # self._cache.remove_open_order(order)
            if order_dict.status == ORDER_STATUS.FILLED:
                order._status = order_dict.status
                del self._open_orders[order_dict.order_id]
                self._orders_waiting_for_trade[order_dict.order_id] = order
                # self._cache.remove_open_order(order)

    def on_trade(self, trade_dict):
        system_log.info('CTPTradeGateway: on trade {} of {} by {}'.format(trade_dict.trade_id, trade_dict.order_id, self._run_id))
        self._cache.cache_trade(trade_dict)

        try:
            account = Environment.get_instance().get_account(trade_dict.order_book_id)
        except AttributeError:
            # rqalpha 还未初始化
            return

        if trade_dict.trade_id in account._backward_trade_set:
            return

        if trade_dict.order_id in self._open_orders:
            order = copy(self._open_orders[trade_dict.order_id])
            del self._open_orders[trade_dict.order_id]
        elif trade_dict.order_id in self._orders_waiting_for_trade:
            order = copy(self._orders_waiting_for_trade[trade_dict.order_id])
            del self._orders_waiting_for_trade[trade_dict.order_id]
        else:
            order = None

        if order is None:
            user_system_log.warn(
                'Trade {}, whose order {} was not submitted by current strategy, will be ignored. '
                'As a result, account and position data in strategy may be inconsistent with CTP.'.format(
                    str(trade_dict.trade_id), str(trade_dict.order_id)
                )
            )
            return

        commission = cal_commission(trade_dict, order.position_effect)
        trade = Trade.__from_create__(
            trade_dict.order_id, trade_dict.price, trade_dict.quantity,
            trade_dict.side, trade_dict.position_effect, trade_dict.order_book_id, trade_id=trade_dict.trade_id,
            commission=commission, frozen_price=trade_dict.price)

        order.fill(trade)
        self._que.push(RqEvent(EVENT.TRADE, account=account, trade=trade))

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
            user_system_log.error('Data request timeout.')
            raise RuntimeError('Data request timeout.')

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
            user_system_log.error('Data request timeout.')
            raise RuntimeError('Data request timeout.')

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
                self._open_orders = {}
                for order_dict in order_cache.values():
                    if order_dict.status == ORDER_STATUS.ACTIVE:
                        order = Order.__from_create__(
                            order_dict.order_book_id, order_dict.quantity, order_dict.side, order_dict.style, order_dict.position_effect
                        )
                        order._calendar_dt = order_dict.calendar_dt
                        order._order_id = order_dict.order_id
                        self._open_orders[order_dict.order_id] = order
                break

    @property
    def open_orders(self):
        for order in itervalues(self._open_orders):
            if order._trading_dt is None and order._calendar_dt is not None:
                # data_proxy 初始化在 start_up 之后
                order._trading_dt = Environment.get_instance().data_proxy.get_trading_dt(order._calendar_dt)
        return list(itervalues(self._open_orders))

    @staticmethod
    def on_debug(debug):
        system_log.debug(debug)

    @staticmethod
    def on_err(error, func_name):
        system_log.error('CTP 错误，错误代码：%s，错误信息：%s' % (str(error.ErrorID), error.ErrorMsg.decode('GBK')))


class DataCache(object):
    def __init__(self):
        self.ins = {}
        self.future_info = {}

        self.trades = {}

        self.pos = {}

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

    def cache_position(self, pos_cache):
        self.pos = pos_cache

    def cache_account(self, account_dict):
        self._account_dict = account_dict

    def cache_trade(self, trade_dict):
        if trade_dict.order_book_id not in self.trades:
            self.trades[trade_dict.order_book_id] = []
        self.trades[trade_dict.order_book_id].append(trade_dict)

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

            position._last_price = pos_dict.prev_settle_price

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
