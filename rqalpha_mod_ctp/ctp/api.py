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
from threading import Lock
from functools import partial
from six import iterkeys
from click import progressbar

from rqalpha.const import SIDE, POSITION_EFFECT, ORDER_STATUS, COMMISSION_TYPE, MARGIN_TYPE
from rqalpha.utils.datetime_func import convert_date_time_ms_int_to_datetime

from .pyctp import MdApi, TraderApi, ApiStruct
from .utils import is_future, make_order_book_id, bytes2str, str2bytes, Status, api_decorator
from .utils import ORDER_TYPE_MAPPING, POSITION_EFFECT_MAPPING, SIDE_MAPPING, ORDER_TYPE_REVERSE, SIDE_REVERSE
from .utils import retry_and_find_result, query_and_find_result


class ApiMixIn(object):
    def __init__(self, name, user_id, password, broker_id, frontend_url, logger):
        self.name = name
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.frontend_url = frontend_url
        self.logger = logger

        self._status = Status.NOT_INITIALIZED
        self._status_msg = None
        self._req_id = 0

    def do_init(self):
        pass

    def prepare(self):
        pass

    def close(self):
        pass

    def register_queries(self):
        pass

    def start_up(self):
        if self._status == Status.ERROR:
            self._status = Status.NOT_INITIALIZED

        if not (self.user_id and self.password and self.broker_id and self.frontend_url):
            raise RuntimeError("{}: invalid parameters.")

        init_result = retry_and_find_result(
            self.do_init,
            lambda: self._status >= Status.INITIALIZED,
            lambda: self._status == Status.ERROR
        )
        if init_result is None:
            self._status = Status.ERROR
            raise RuntimeError("{}: init timeout.".format(self.name))
        elif not init_result:
            raise RuntimeError(self._status_msg)

        self.logger.debug('{}: init successfully.'.format(self.name))

    def tear_down(self):
        self.close()
        self._status = Status.NOT_INITIALIZED
        self.logger.info('{}: torn up'.format(self.name))

    @property
    def req_id(self):
        self._req_id += 1
        return self._req_id


class CtpMdApi(MdApi, ApiMixIn):
    def __init__(self, user_id, password, broker_id, md_frontend_url, logger):
        MdApi.__init__(self)
        ApiMixIn.__init__(self, 'CtpMdApi', user_id, password, broker_id, md_frontend_url, logger)

        self.on_tick = None
        self.get_instrument_ids = None

    def do_init(self):
        self.Create()
        self.RegisterFront(str2bytes(self.frontend_url))
        self.Init()

    def close(self):
        self.Release()

    def start_up(self):
        ApiMixIn.start_up(self)
        self._status = Status.RUNNING

    @api_decorator(check_status=False)
    def OnFrontConnected(self):
        """服务器连接"""
        self.ReqUserLogin(ApiStruct.ReqUserLogin(
            BrokerID=str2bytes(self.broker_id),
            UserID=str2bytes(self.user_id),
            Password=str2bytes(self.password)
        ), self.req_id)

    @api_decorator(check_status=False)
    def OnFrontDisconnected(self, nReason):
        """服务器断开"""
        self._status = Status.NOT_INITIALIZED

    @api_decorator(check_status=False)
    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        """错误回报"""
        self.logger.error('{}: OnRspError: {}, {}'.format(self.name, bytes2str(pRspInfo.ErrorMsg), pRspInfo.ErrorID))

    @api_decorator(check_status=False)
    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登陆回报"""
        self.logger.debug('{}: OnRspUserLogin: {}, pRspInfo: {}'.format(self.name, pRspUserLogin, pRspInfo))
        if pRspInfo.ErrorID == 0:
            self.SubscribeMarketData([str2bytes(i) for i in self.get_instrument_ids()])
            self._status = Status.INITIALIZED
        else:
            self._status = Status.ERROR
            self._status_msg = bytes2str(pRspInfo.ErrorMsg)

    @api_decorator()
    def OnRtnDepthMarketData(self, pDepthMarketData):
        """行情推送"""
        try:
            tick_date = int(pDepthMarketData.TradingDay)
            tick_time = int(
                        (bytes2str(pDepthMarketData.UpdateTime).replace(':', ''))
                    ) * 1000 + int(pDepthMarketData.UpdateMillisec)

            self.on_tick({
                'order_book_id': make_order_book_id(pDepthMarketData.InstrumentID),
                'datetime': convert_date_time_ms_int_to_datetime(tick_date, tick_time),
                # date and time field should be removed
                'date': tick_date,
                'time': tick_time,
                'open': pDepthMarketData.OpenPrice,
                'last': pDepthMarketData.LastPrice,
                'low': pDepthMarketData.LowestPrice,
                'high': pDepthMarketData.HighestPrice,
                'prev_close': pDepthMarketData.PreClosePrice,
                'volume': pDepthMarketData.Volume,
                'total_turnover': pDepthMarketData.Turnover,
                'open_interest': pDepthMarketData.OpenInterest,
                'prev_settlement': pDepthMarketData.SettlementPrice,
                'b1': pDepthMarketData.BidPrice1,
                'b2': pDepthMarketData.BidPrice2,
                'b3': pDepthMarketData.BidPrice3,
                'b4': pDepthMarketData.BidPrice4,
                'b5': pDepthMarketData.BidPrice5,
                'b1_v': pDepthMarketData.BidVolume1,
                'b2_v': pDepthMarketData.BidVolume2,
                'b3_v': pDepthMarketData.BidVolume3,
                'b4_v': pDepthMarketData.BidVolume4,
                'b5_v': pDepthMarketData.BidVolume5,
                'a1': pDepthMarketData.AskPrice1,
                'a2': pDepthMarketData.AskPrice2,
                'a3': pDepthMarketData.AskPrice3,
                'a4': pDepthMarketData.AskPrice4,
                'a5': pDepthMarketData.AskPrice5,
                'a1_v': pDepthMarketData.AskVolume1,
                'a2_v': pDepthMarketData.AskVolume2,
                'a3_v': pDepthMarketData.AskVolume3,
                'a4_v': pDepthMarketData.AskVolume4,
                'a5_v': pDepthMarketData.AskVolume5,

                'limit_up': pDepthMarketData.UpperLimitPrice,
                'limit_down': pDepthMarketData.LowerLimitPrice,
            })
        except ValueError:
            pass


class CtpTradeApi(TraderApi, ApiMixIn):
    # TODO: 流文件放在不同路径(con)
    def __init__(self, user_id, password, broker_id, md_frontend_url, logger, qry_account, qry_commission, show_progress_bar):
        TraderApi.__init__(self)
        ApiMixIn.__init__(self, 'CtpTradeApi', user_id, password, broker_id, md_frontend_url, logger)

        self._session_id = None
        self._front_id = None

        self._ins_cache = None
        self._account_cache = None
        self._order_cache = None

        self._current_query_done = False
        self._queries = [self.qry_open_orders]
        if qry_account:
            self._queries.extend([self.qry_account, self.qry_positions, self.qry_trades])

        self._qry_commission = qry_commission
        self._current_query_commission_obid = None
        self._show_progress_bar = show_progress_bar

        self._lock = Lock()

        self.on_order_status_updated = None
        self.on_order_cancel_failed = None
        self.on_trade = None

        self.on_commission = None

    def do_init(self):
        self.Create()
        self.SubscribePrivateTopic(ApiStruct.TERT_RESTART)
        self.SubscribePublicTopic(ApiStruct.TERT_RESTART)
        self.RegisterFront(str2bytes(self.frontend_url))
        self.Init()

    def close(self):
        self.Release()

    def start_up(self):
        ApiMixIn.start_up(self)

        query_and_find_result(self.qry_instruments, lambda: self._current_query_done, lambda: self._status == Status.ERROR)

        print(len(self._queries))
        if self._show_progress_bar and len(self._queries) > 1:
            progress_bar = progressbar(length=len(self._queries), label='Querying data')
        else:
            progress_bar = None
        for query in self._queries:
            sleep(1)
            self._current_query_done = False
            result = query_and_find_result(query, lambda: self._current_query_done, lambda: self._status == Status.ERROR)
            if not result:
                raise RuntimeError("{}: {} failed.".format(self.name, query.__name__))
            if progress_bar:
                progress_bar.update(1)
        if progress_bar:
            progress_bar.render_finish()
        self._status = Status.RUNNING

    @api_decorator(check_status=False)
    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登陆回报"""
        if pRspInfo.ErrorID == 0:
            self._front_id = pRspUserLogin.FrontID
            self._session_id = pRspUserLogin.SessionID
            self._status = Status.INITIALIZED
            self.ReqSettlementInfoConfirm(ApiStruct.SettlementInfoConfirm(
                BrokerID=str2bytes(self.broker_id), InvestorID=str2bytes(self.user_id)
            ), self.req_id)
        else:
            self._status = Status.ERROR
            self._status_msg = bytes2str(pRspInfo.ErrorMsg)

    @api_decorator(check_status=False)
    def OnFrontConnected(self):
        self.ReqUserLogin(ApiStruct.ReqUserLogin(
            UserID=str2bytes(self.user_id),
            BrokerID=str2bytes(self.broker_id),
            Password=str2bytes(self.password),
        ), self.req_id)

    @api_decorator(check_status=False)
    def OnFrontDisconnected(self, nReason):
        self._status = Status.NOT_INITIALIZED

    @api_decorator()
    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        if not pInputOrder.OrderRef:
            return
        self.on_order_status_updated(int(pInputOrder.OrderRef), ORDER_STATUS.REJECTED, bytes2str(pRspInfo.ErrorMsg))

    @api_decorator()
    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        if not pInputOrder.OrderRef:
            return
        self.on_order_status_updated(int(pInputOrder.OrderRef), ORDER_STATUS.REJECTED, bytes2str(pRspInfo.ErrorMsg))

    @api_decorator()
    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        if not pInputOrderAction.OrderRef:
            return
        if pRspInfo.ErrorID == 0:
            return
        self.on_order_cancel_failed(int(pInputOrderAction.OrderRef), bytes2str(pRspInfo.ErrorMsg))

    @api_decorator()
    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.logger.error('{}: OnRspError: {}, {}'.format(self.name, bytes2str(pRspInfo.ErrorMsg), pRspInfo.ErrorID))

    @api_decorator()
    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        self.on_order_cancel_failed(int(pOrderAction.OrderRef), bytes2str(pRspInfo.ErrorMsg))

    @api_decorator()
    def OnRtnOrder(self, pOrder):
        if pOrder.OrderStatus in [ApiStruct.OST_PartTradedQueueing, ApiStruct.OST_NoTradeQueueing]:
            status = ORDER_STATUS.ACTIVE
        elif pOrder.OrderStatus == ApiStruct.OST_AllTraded:
            status = ORDER_STATUS.FILLED
        elif pOrder.OrderStatus == ApiStruct.OST_Canceled:
            if bytes2str(pOrder.StatusMsg) == '已撤单':
                status = ORDER_STATUS.CANCELLED
            else:
                status = ORDER_STATUS.REJECTED
        elif pOrder.OrderStatus == ApiStruct.OST_Unknown:
            return
        else:
            self.logger.error('{}: Order {} has an unrecognized order status: {}'.format(
                self.name, int(pOrder.OrderRef), pOrder.OrderStatus
            ))
            return

        self.on_order_status_updated(
            int(pOrder.OrderRef),
            status,
            bytes2str(pOrder.StatusMsg),
            bytes2str(pOrder.OrderSysID).strip()
        )

    @api_decorator()
    def OnRtnTrade(self, pTrade):
        self.on_trade(
            int(pTrade.OrderRef),
            int(pTrade.TradeID),
            float(pTrade.Price),
            float(pTrade.Volume),
            int(pTrade.TradeTime.replace(b':', b'')) * 1000,
            int(pTrade.TradeDate),
            int(pTrade.TradingDay),
            bytes2str(pTrade.OrderSysID).strip(),
        )

    @api_decorator(check_status=False)
    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        if is_future(pInstrument.InstrumentID):
            order_book_id = make_order_book_id(pInstrument.InstrumentID)
            with self._lock:
                self._ins_cache[order_book_id] = {
                    'order_book_id': order_book_id,
                    'instrument_id': str2bytes(pInstrument.InstrumentID),
                    'exchange_id': str2bytes(pInstrument.ExchangeID),
                    'tick_size': float(pInstrument.PriceTick),
                    'contract_multiplier': pInstrument.VolumeMultiple,
                    'long_margin_ratio': pInstrument.LongMarginRatio,
                    'short_margin_ratio': pInstrument.ShortMarginRatio,
                    "margin_type": MARGIN_TYPE.BY_MONEY,
                }
        if bIsLast:
            with self._lock:
                if self._qry_commission:
                    for order_book_id in iterkeys(self._ins_cache):
                        self._queries.append(partial(self.qry_commission, order_book_id=order_book_id))
                self._current_query_done = True

    @api_decorator(check_status=False)
    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        if pOrder and pOrder.OrderStatus in [ApiStruct.OST_PartTradedQueueing, ApiStruct.OST_NoTradeQueueing]:
            with self._lock:
                self._order_cache[int(pOrder.OrderRef)] = {
                    'status': ORDER_STATUS.ACTIVE,
                    'order_book_id': make_order_book_id(pOrder.InstrumentID),
                    'quantity': pOrder.VolumeTotalOriginal,
                    'filled_quantity': pOrder.VolumeTotalTraded,
                    'side': SIDE_REVERSE.get(pOrder.Direction, SIDE.BUY),
                    'position_effect': {
                        ApiStruct.OF_Open: POSITION_EFFECT.OPEN,
                        ApiStruct.OF_CloseToday: POSITION_EFFECT.CLOSE_TODAY,
                    }.get(pOrder.CombOffsetFlag, POSITION_EFFECT.CLOSE) if pOrder.ExchangeID == 'SHFE' else {
                        ApiStruct.OF_Open: POSITION_EFFECT.OPEN
                    }.get(pOrder.CombOffsetFlag, POSITION_EFFECT.CLOSE),
                    'price': pOrder.LimitPrice,
                    'type': ORDER_TYPE_REVERSE.get(pOrder.OrderPriceType),
                }
        if bIsLast:
            with self._lock:
                self._current_query_done = True

    @api_decorator(check_status=False)
    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        yesterday_portfolio_value = pTradingAccount.PreBalance if pTradingAccount.PreBalance else pTradingAccount.Balance
        with self._lock:
            self._account_cache['total_cash'] += yesterday_portfolio_value
            self._current_query_done = True

    @api_decorator(check_status=False)
    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        """持仓查询回报"""
        if pInvestorPosition and pInvestorPosition.InstrumentID:
            order_book_id = make_order_book_id(pInvestorPosition.InstrumentID)
            try:
                ins_dict = self._ins_cache[order_book_id]
            except KeyError:
                self._status = Status.ERROR
                self._status_msg = 'Query instrument failed.'
                return
            with self._lock:
                position = self._account_cache['positions'].setdefault(order_book_id, {
                    'order_book_id': order_book_id,
                    'buy_old_holding_list': [],
                    'sell_old_holding_list': [],
                    'buy_today_holding_list': [],
                    'sell_today_holding_list': [],
                    'buy_transaction_cost': 0,
                    'sell_transaction_cost': 0,
                    'buy_realized_pnl': 0,
                    'sell_realized_pnl': 0,
                    'buy_avg_open_price': 0,
                    'sell_avg_open_price': 0,
                    'margin_rate': ins_dict['long_margin_ratio'],

                    'buy_quantity': 0,
                    'sell_quantity': 0,
                    'buy_open_cost': 0,
                    'sell_open_cost': 0,

                    'prev_settlement_price': 0,
                })
                position['prev_settlement_price'] = pInvestorPosition.PreSettlementPrice
                if pInvestorPosition.PosiDirection in [ApiStruct.PD_Net, ApiStruct.PD_Long]:
                    position['buy_old_holding_list'] = [(
                        pInvestorPosition.PreSettlementPrice, pInvestorPosition.Position - pInvestorPosition.TodayPosition
                    )]
                    position['buy_transaction_cost'] += pInvestorPosition.Commission
                    position['buy_realized_pnl'] += pInvestorPosition.CloseProfit

                    position['buy_open_cost'] += pInvestorPosition.OpenCost
                    position['buy_quantity'] += pInvestorPosition.Position
                    if position['buy_quantity']:
                        position['buy_avg_open_price'] = position['buy_open_cost'] / (
                        position['buy_quantity'] * ins_dict['contract_multiplier'])
                elif pInvestorPosition.PosiDirection == ApiStruct.PD_Short:
                    position['sell_old_holding_list'] = [(
                        pInvestorPosition.PreSettlementPrice, pInvestorPosition.Position - pInvestorPosition.TodayPosition
                    )]
                    position['sell_transaction_cost'] += pInvestorPosition.Commission
                    position['sell_realized_pnl'] += pInvestorPosition.CloseProfit

                    position['sell_open_cost'] += pInvestorPosition.OpenCost
                    position['sell_quantity'] += pInvestorPosition.Position
                    if position['sell_quantity']:
                        position['sell_avg_open_price'] = position['sell_open_cost'] / (
                        position['sell_quantity'] * ins_dict['contract_multiplier'])
                else:
                    self.logger.error('{}: Unknown direction: {}'.format(self.name, pInvestorPosition.PosiDirection))
        if bIsLast:
            with self._lock:
                self._current_query_done = True

    @api_decorator(check_status=False)
    def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast):
        if pTrade:
            trade_id = int(pTrade.TradeID)
            with self._lock:
                if trade_id not in self._account_cache['backward_trade_set']:
                    self._account_cache['backward_trade_set'].append(trade_id)
        if bIsLast:
            self._current_query_done = True

    @api_decorator(check_status=False)
    def OnRspQryInstrumentCommissionRate(self, pInstrumentCommissionRate, pRspInfo, nRequestID, bIsLast):
        with self._lock:
            ins_cache = self._ins_cache[self._current_query_commission_obid]
            if pInstrumentCommissionRate.OpenRatioByMoney == 0 and pInstrumentCommissionRate.CloseRatioByMoney:
                ins_cache['open_commission_ratio'] = pInstrumentCommissionRate.OpenRatioByVolume
                ins_cache['close_commission_ratio'] = pInstrumentCommissionRate.CloseRatioByVolume
                ins_cache['close_commission_today_ratio'] = pInstrumentCommissionRate.CloseTodayRatioByVolume
                if pInstrumentCommissionRate.OpenRatioByVolume != 0 or pInstrumentCommissionRate.CloseRatioByVolume != 0:
                    ins_cache['commission_type'] = COMMISSION_TYPE.BY_VOLUME
                else:
                    ins_cache['commission_type'] = None
            else:
                ins_cache['open_commission_ratio'] = pInstrumentCommissionRate.OpenRatioByMoney
                ins_cache['close_commission_ratio'] = pInstrumentCommissionRate.CloseRatioByMoney
                ins_cache['close_commission_today_ratio'] = pInstrumentCommissionRate.CloseTodayRatioByMoney
                if pInstrumentCommissionRate.OpenRatioByVolume == 0 and pInstrumentCommissionRate.CloseRatioByVolume== 0:
                    ins_cache['commission_type'] = COMMISSION_TYPE.BY_MONEY
                else:
                    ins_cache['commission_type'] = None
            self._current_query_done = True

    def qry_instruments(self):
        with self._lock:
            self._ins_cache = {}
            self.ReqQryInstrument(ApiStruct.QryInstrument(), self.req_id)

    def qry_open_orders(self):
        with self._lock:
            self._order_cache = {}
            self.ReqQryOrder(ApiStruct.QryOrder(
                BrokerID=str2bytes(self.broker_id),
                InvestorID=str2bytes(self.user_id)
            ), self.req_id)

    def qry_account(self):
        def cal_margin(order_book_id, quantity, price, side):
            ins_dict = self._ins_cache[order_book_id]
            if side == SIDE.BUY:
                return quantity * ins_dict['contract_multiplier'] * price * ins_dict['long_margin_ratio']
            else:
                return quantity * ins_dict['contract_multiplier'] * price * ins_dict['short_margin_ratio']

        with self._lock:
            self._account_cache = {
                'frozen_cash': sum(cal_margin(
                    o['order_book_id'], o['quantity'] - o['filled_quantity'], o['price'], o['side']
                ) for o in self._order_cache),
                'total_cash': 0,
                'transaction_cost': 0,
                'positions': {},
                'backward_trade_set': [],
            }
            self.ReqQryTradingAccount(ApiStruct.QryTradingAccount(), self.req_id)

    def qry_positions(self):
        with self._lock:
            self.ReqQryInvestorPosition(ApiStruct.QryInvestorPosition(
                BrokerID=str2bytes(self.broker_id),
                InvestorID=str2bytes(self.user_id)
            ), self.req_id)

    def qry_trades(self):
        with self._lock:
            self.ReqQryTrade(ApiStruct.QryTrade(
                BrokerID=str2bytes(self.broker_id),
                InvestorID=str2bytes(self.user_id),
            ), self.req_id)

    def qry_commission(self, order_book_id):
        ins_dict = self._ins_cache.get(order_book_id)
        if ins_dict is None:
            self._current_query_done = True
            return
        with self._lock:
            self._current_query_commission_obid = order_book_id
            self.ReqQryInstrumentCommissionRate(ApiStruct.QryInstrumentCommissionRate(
                InstrumentID=str2bytes(ins_dict['instrument_id']),
                InvestorID=str2bytes(self.user_id),
                BrokerID=str2bytes(self.broker_id),
            ), self.req_id)

    @api_decorator(log=False, raise_error=True)
    def submit_order(self, order_ref, order):
        self.logger.debug('{}: SubmitOrder: {}'.format(self.name, order))
        ins_dict = self._ins_cache.get(order.order_book_id)
        if ins_dict is None:
            raise RuntimeError(
                'Account is not inited successfully or instrument {} is not trading.'.format(order.order_book_id)
            )
        try:
            price_type = ORDER_TYPE_MAPPING[order.type]
        except KeyError:
            raise RuntimeError('Order type {} is not supported by ctp api.'.format(order.type))
        try:
            position_effect = POSITION_EFFECT_MAPPING[order.position_effect]
        except KeyError:
            raise RuntimeError('Order position effect {} is not supported by ctp api.'.format(order.position_effect))

        req_id = self.req_id
        req = ApiStruct.InputOrder(
            InstrumentID=str2bytes(ins_dict['instrument_id']),
            LimitPrice=float(order.price),
            VolumeTotalOriginal=int(order.quantity),
            OrderPriceType=price_type,
            Direction=SIDE_MAPPING.get(order.side, ''),
            CombOffsetFlag=position_effect,

            OrderRef=str2bytes(str(order_ref)),
            InvestorID=str2bytes(self.user_id),
            UserID=str2bytes(self.user_id),
            BrokerID=str2bytes(self.broker_id),

            CombHedgeFlag=ApiStruct.HF_Speculation,
            ContingentCondition=ApiStruct.CC_Immediately,
            ForceCloseReason=ApiStruct.FCC_NotForceClose,
            IsAutoSuspend=0,
            TimeCondition=ApiStruct.TC_GFD,
            VolumeCondition=ApiStruct.VC_AV,
            MinVolume=1,

            RequestID=req_id,
        )
        self.ReqOrderInsert(req, req_id)

    @api_decorator(log=False, raise_error=True)
    def cancel_order(self, order_ref, order):
        self.logger.debug('{}: CancelOrder: {}'.format(self.name, order))
        ins_dict = self._ins_cache.get(order.order_book_id)
        if ins_dict is None:
            raise RuntimeError('Account is not inited successfully or instrument {} is not trading.'.format(order.order_book_id))

        req_id = self.req_id
        req = ApiStruct.InputOrderAction(
            # TODO: whether InstrumentID and ExchangeID are necessary?
            InstrumentID=str2bytes(ins_dict['instrument_id']),
            ExchangeID=str2bytes(ins_dict['exchange_id']),
            OrderRef=str2bytes(str(order_ref)),
            FrontID=int(self._front_id),
            SessionID=int(order.session_id),

            ActionFlag=ApiStruct.AF_Delete,
            BrokerID=str2bytes(self.broker_id),
            InvestorID=str2bytes(self.user_id),

            RequestID=req_id,
        )
        self.ReqOrderAction(req, req_id)

    @property
    def future_infos(self):
        return self._ins_cache

    @property
    def open_orders(self):
        return self._order_cache

    @property
    def account_state(self):
        return self._account_cache
