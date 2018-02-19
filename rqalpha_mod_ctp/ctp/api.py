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

from rqalpha.const import SIDE, POSITION_EFFECT, ORDER_STATUS
from rqalpha.utils.datetime_func import convert_date_time_ms_int_to_datetime

from .pyctp import MdApi, TraderApi, ApiStruct
from .utils import is_future, make_order_book_id, bytes2str, str2bytes, Status, api_decorator
from .utils import ORDER_TYPE_MAPPING, POSITION_EFFECT_MAPPING, SIDE_MAPPING, ORDER_TYPE_REVERSE, SIDE_REVERSE


class ApiMixIn(object):
    def __init__(self, name, user_id, password, broker_id, frontend_url, logger):
        self.name = name
        self.user_id = user_id
        self.password = password
        self.broker_id = broker_id
        self.frontend_url = frontend_url
        self.logger = logger

        self._status = Status.DISCONNECTED
        self._starting_up = False
        self._status_msg = None
        self._req_id = 0

    def do_init(self):
        pass

    def prepare(self):
        pass

    def close(self):
        pass

    def start_up(self, retry_time=5):
        if self._starting_up:
            return

        self._starting_up = True
        if self._status == Status.ERROR:
            self._status = Status.DISCONNECTED

        if self.user_id and self.password and self.broker_id and self.frontend_url:
            self.logger.info('{}: start up'.format(self.name))

        for i in range(retry_time + 2):
            if self._status >= Status.PREPARING:
                break
            self.do_init()
            for j in range(100 * 2 ** i):
                sleep(0.01)
                if self._status >= Status.PREPARING:
                    break
                elif self._status == Status.ERROR:
                    self._starting_up = False
                    raise RuntimeError(self._status_msg)
            else:
                continue
            break
        else:
            self._status = Status.ERROR
            self._starting_up = False
            raise RuntimeError('{}: init timeout'.format(self.name))
        self.logger.debug('{}: init successfully'.format(self.name))

        for i in range(retry_time + 1):
            if self._status >= Status.RUNNING:
                break
            self.prepare()
            for j in range(100 * 2 ** i):
                sleep(0.01)
                if self._status >= Status.RUNNING:
                    break
                elif self._status == Status.ERROR:
                    self._starting_up = False
                    raise RuntimeError(self._status_msg)
            else:
                continue
            break
        else:
            self._status = Status.ERROR
            self._starting_up = False
            raise RuntimeError('{}: prepare timeout'.format(self.name))
        self.logger.info('{}: started up'.format(self.name))
        self._starting_up = False

    def tear_down(self):
        self.close()
        self._status = Status.DISCONNECTED
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

    def prepare(self):
        self.SubscribeMarketData([str2bytes(i) for i in self.get_instrument_ids()])
        self._status = Status.RUNNING

    def close(self):
        self.Release()

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
        self._status = Status.DISCONNECTED

    @api_decorator(check_status=False)
    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        """错误回报"""
        self.logger.error('{}: OnRspError: {}, {}'.format(self.name, bytes2str(pRspInfo.ErrorMsg), pRspInfo.ErrorID))

    @api_decorator(check_status=False)
    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登陆回报"""
        self.logger.debug('{}: OnRspUserLogin: {}, pRspInfo: {}'.format(self.name, pRspUserLogin, pRspInfo))
        if pRspInfo.ErrorID == 0:
            self._status = Status.PREPARING
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
    def __init__(self, user_id, password, broker_id, md_frontend_url, logger):
        TraderApi.__init__(self)
        ApiMixIn.__init__(self, 'CtpTradeApi', user_id, password, broker_id, md_frontend_url, logger)

        self._session_id = None
        self._front_id = None

        self._ins_cache = {}
        self._account_cache = {}
        self._order_cache = {}

        self.on_order_status_updated = None
        self.on_order_cancel_failed = None
        self.on_trade = None

        self.on_qry_open_orders = None
        self.on_qry_account = None
        self.on_qry_position = None

        self.on_commission = None

    def do_init(self):
        self.Create()
        self.SubscribePrivateTopic(ApiStruct.TERT_RESTART)
        self.SubscribePublicTopic(ApiStruct.TERT_RESTART)
        self.RegisterFront(str2bytes(self.frontend_url))
        self.Init()

    def prepare(self):
        self.ReqQryInstrument(ApiStruct.QryInstrument(), self.req_id)

    def close(self):
        self.Release()

    @api_decorator(check_status=False)
    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登陆回报"""
        if pRspInfo.ErrorID == 0:
            self._front_id = pRspUserLogin.FrontID
            self._session_id = pRspUserLogin.SessionID
            self._status = Status.PREPARING
            self.ReqSettlementInfoConfirm(ApiStruct.SettlementInfoConfirm(
                BrokerID=str2bytes(self.broker_id), InvestorID=str2bytes(self.user_id)
            ), self.req_id)
        else:
            self._status = Status.ERROR
            self._status_msg = bytes2str(pRspInfo.ErrorMsg)

    @api_decorator(check_status=False)
    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        if is_future(pInstrument.InstrumentID):
            order_book_id = make_order_book_id(pInstrument.InstrumentID)
            self._ins_cache[order_book_id] = {
                'order_book_id': order_book_id,
                'instrument_id': str2bytes(pInstrument.InstrumentID),
                'exchange_id': str2bytes(pInstrument.ExchangeID),
                'tick_size': float(pInstrument.PriceTick),
                'contract_multiplier': pInstrument.VolumeMultiple,
                'long_margin_rate': pInstrument.LongMarginRatio,
                'short_margin_rate': pInstrument.ShortMarginRatio,
            }
        if bIsLast:
            sleep(0.5)
            self.ReqQryOrder(ApiStruct.QryOrder(
                BrokerID=str2bytes(self.broker_id),
                InvestorID=str2bytes(self.user_id)
            ), self.req_id)

    @api_decorator(check_status=False)
    def OnRspQryOrder(self, pOrder, pRspInfo, nRequestID, bIsLast):
        if pOrder:
            if pOrder.OrderStatus in [ApiStruct.OST_PartTradedQueueing, ApiStruct.OST_NoTradeQueueing]:
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
            self.logger.debug('{}: Last OnRspQryOrder'.format(self.name))
            self._status = Status.RUNNING

    @api_decorator(check_status=False)
    def OnFrontConnected(self):
        self.ReqUserLogin(ApiStruct.ReqUserLogin(
            UserID=str2bytes(self.user_id),
            BrokerID=str2bytes(self.broker_id),
            Password=str2bytes(self.password),
        ), self.req_id)

    @api_decorator(check_status=False)
    def OnFrontDisconnected(self, nReason):
        self._status = Status.DISCONNECTED

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
    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        yesterday_portfolio_value = pTradingAccount.PreBalance if pTradingAccount.PreBalance else pTradingAccount.Balance
        self._account_cache['total_cash'] += yesterday_portfolio_value
        sleep(1)
        self.ReqQryInvestorPosition(ApiStruct.QryInvestorPosition(
            BrokerID=str2bytes(self.broker_id),
            InvestorID=str2bytes(self.user_id)
        ), self.req_id)
        sleep(1)
        self.ReqQryTrade(ApiStruct.QryTrade(
            BrokerID=str2bytes(self.broker_id),
            InvestorID=str2bytes(self.user_id),
        ), self.req_id)

    @api_decorator(check_status=False)
    def OnRspQryTrade(self, pTrade, pRspInfo, nRequestID, bIsLast):
        # TODO
        pass

    @api_decorator(check_status=False)
    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        """持仓查询回报"""

        if not pInvestorPosition:
            return
        if not pInvestorPosition.InstrumentID:
            return
        order_book_id = make_order_book_id(pInvestorPosition.InstrumentID)
        ins_dict = self._ins_cache[order_book_id]
        position = self._account_cache['positions'].setdefault(order_book_id, {
            'order_book_id': order_book_id,
            'buy_old_holding_list': [],
            'sell_old_holding_list': [],
            'buy_today_holding_list': [],
            'sell_today_holding_list':[],
            'buy_transaction_cost': 0,
            'sell_transaction_cost': 0,
            'buy_realized_pnl': 0,
            'sell_realized_pnl': 0,
            'buy_avg_open_price': 0,
            'sell_avg_open_price': 0,
            'margin_rate': ins_dict['long_margin_rate'],

            'buy_quantity': 0,
            'sell_quantity': 0,
            'buy_open_cost': 0,
            'sell_open_cost': 0,
        })
        if pInvestorPosition.PosiDirection in [ApiStruct.PD_Net, ApiStruct.PD_Long]:
            position['buy_old_holding_list'] = [(
                pInvestorPosition.PreSettlementPrice, pInvestorPosition.Position - pInvestorPosition.TodayPosition
            )]
            position['buy_transaction_cost'] += pInvestorPosition.Commission
            position['buy_realized_pnl'] += pInvestorPosition.CloseProfit

            position['buy_open_cost'] += pInvestorPosition.OpenCost
            position['buy_quantity'] += pInvestorPosition.Position
            if position['buy_quantity']:
                position['buy_avg_open_price'] = position['buy_open_cost'] / (position['buy_quantity'] * ins_dict['contract_multiplier'])
        elif pInvestorPosition.PosiDirection == ApiStruct.PD_Short:
            position['sell_old_holding_list'] = [(
                pInvestorPosition.PreSettlementPrice, pInvestorPosition.Position - pInvestorPosition.TodayPosition
            )]
            position['sell_transaction_cost'] += pInvestorPosition.Commission
            position['sell_realized_pnl'] += pInvestorPosition.CloseProfit

            position['sell_open_cost'] += pInvestorPosition.OpenCost
            position['sell_quantity'] += pInvestorPosition.Position
            if position['sell_quantity']:
                position['sell_avg_open_price'] = position['sell_open_cost'] / (position['sell_quantity'] * ins_dict['contract_multiplier'])
        else:
            self.logger.error('{}: Unknown direction: {}'.format(self.name, pInvestorPosition.PosiDirection))

    def qry_account(self):
        def cal_margin(order_book_id, quantity, price, side):
            ins_dict = self._ins_cache[order_book_id]
            if side == SIDE.BUY:
                return quantity * ins_dict['contract_multiplier'] * price * ins_dict['long_margin_rate']
            else:
                return quantity * ins_dict['contract_multiplier'] * price * ins_dict['short_margin_rate']

        self._account_cache = {
            'frozen_cash': sum(cal_margin(
                o['order_book_id'], o['quantity'] - o['filled_quantity'], o['price'], o['side']
            ) for o in self._order_cache),
            'total_cash': 0,
            'transaction_cost': 0,
            'positions': dict(),
            'backward_trade_set': set(),
        }
        self.ReqQryTradingAccount(ApiStruct.QryTradingAccount(), self.req_id)

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
