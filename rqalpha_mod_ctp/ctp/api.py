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

from rqalpha.const import ORDER_TYPE, SIDE, POSITION_EFFECT, ORDER_STATUS

from .pyctp import MdApi, TraderApi, ApiStruct
from .data_dict import TickDict
from ..utils import make_order_book_id, str2bytes, bytes2str, is_future

ORDER_TYPE_MAPPING = {
    ORDER_TYPE.MARKET: ApiStruct.OPT_AnyPrice,
    ORDER_TYPE.LIMIT: ApiStruct.OPT_LimitPrice,
}

SIDE_MAPPING = {
    SIDE.BUY: ApiStruct.D_Buy,
    SIDE.SELL: ApiStruct.D_Sell,
}

POSITION_EFFECT_MAPPING = {
    POSITION_EFFECT.OPEN: ApiStruct.OF_Open,
    POSITION_EFFECT.CLOSE: ApiStruct.OF_Close,
    POSITION_EFFECT.CLOSE_TODAY: ApiStruct.OF_CloseToday,
}


class Status(object):
    ERROR = -1
    DISCONNECTED = 0
    PREPARING = 1
    RUNNING = 2


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

        for i in range(retry_time + 1):
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

    def do_init(self):
        self.Create()
        self.RegisterFront(str2bytes(self.frontend_url))
        self.Init()

    def prepare(self):
        self._status = Status.RUNNING

    def close(self):
        self.Release()

    def OnFrontConnected(self):
        """服务器连接"""
        self.ReqUserLogin(ApiStruct.ReqUserLogin(
            BrokerID=str2bytes(self.broker_id),
            UserID=str2bytes(self.user_id),
            Password=str2bytes(self.password)
        ), self.req_id)

    def OnFrontDisconnected(self, nReason):
        """服务器断开"""
        self._status = Status.DISCONNECTED

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        """错误回报"""
        self.logger.warn('{}: OnRspError, ErrID: {}, ErrMsg: {}'.format(
            self.name, pRspInfo.ErrorID, bytes2str(pRspInfo.ErrorMsg)
        ))

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登陆回报"""
        self.logger.debug('{}: OnRspUserLogin: {}, pRspInfo: {}'.format(self.name, pRspUserLogin, pRspInfo))
        if pRspInfo.ErrorID == 0:
            self._status = Status.PREPARING
        else:
            self._status = Status.ERROR
            self._status_msg = bytes2str(pRspInfo.ErrorMsg)

    def OnRtnDepthMarketData(self, pDepthMarketData):
        """行情推送"""
        tick_dict = TickDict(pDepthMarketData)
        if tick_dict.is_valid:
            self.on_tick(tick_dict)


class CtpTradeApi(TraderApi, ApiMixIn):
    # TODO: 流文件放在不同路径(con)
    def __init__(self, user_id, password, broker_id, md_frontend_url, logger):
        TraderApi.__init__(self)
        ApiMixIn.__init__(self, 'CtpTradeApi', user_id, password, broker_id, md_frontend_url, logger)

        self._session_id = None
        self._front_id = None
        self._ins_cache = {}

        self.on_order_status_updated = None
        self.on_order_cancel_failed = None
        self.on_trade = None

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

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        """登陆回报"""
        self.logger.debug('{}: OnRspUserLogin: {}, pRspInfo: {}'.format(self.name, pRspUserLogin, pRspInfo))
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

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        """合约查询回报"""
        if is_future(pInstrument.InstrumentID):
            order_book_id = make_order_book_id(pInstrument.InstrumentID)
            self._ins_cache[order_book_id] = {
                'order_book_id': order_book_id,
                'instrument_id': str2bytes(pInstrument.InstrumentID),
                'exchange_id': str2bytes(pInstrument.ExchangeID),
                'tick_size': float(pInstrument.PriceTick),
                'contract_multiplier': pInstrument.VolumeMultiple,
            }
        if bIsLast:
            self.logger.debug('{}: Last OnRspQryInstrument'.format(self.name))
            self._status = Status.RUNNING

    def OnFrontConnected(self):
        self.logger.debug('{}: OnFrontConnected'.format(self.name))
        self.ReqUserLogin(ApiStruct.ReqUserLogin(
            UserID=str2bytes(self.user_id),
            BrokerID=str2bytes(self.broker_id),
            Password=str2bytes(self.password),
        ), self.req_id)

    def OnFrontDisconnected(self, nReason):
        self.logger.debug('{}: OnFrontConnected'.format(self.name))
        self._status = Status.DISCONNECTED

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        self.logger.debug('{}: OnRspOrderInsert: {}'.format(self.name, pInputOrder))
        if not pInputOrder.OrderRef:
            return
        self.on_order_status_updated(int(pInputOrder.OrderRef), ORDER_STATUS.REJECTED, bytes2str(pRspInfo.ErrorMsg))

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        self.logger.debug('{}: OnErrRtnOrderInsert: {}'.format(self.name, pInputOrder))
        if not pInputOrder.OrderRef:
            return
        self.on_order_status_updated(int(pInputOrder.OrderRef), ORDER_STATUS.REJECTED, bytes2str(pRspInfo.ErrorMsg))

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        self.logger.debug('{}: OnRspOrderAction: {}'.format(self.name, pInputOrderAction))
        if not pInputOrderAction.OrderRef:
            return
        if pRspInfo.ErrorID == 0:
            return
        self.on_order_cancel_failed(int(pInputOrderAction.OrderRef), bytes2str(pRspInfo.ErrorMsg))

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        self.logger.error('{}: OnRspError: {}'.format(self.name, pRspInfo))

    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        self.logger.debug('{}: OnErrRtnOrderAction: {}'.format(self.name, pOrderAction))
        self.on_order_cancel_failed(int(pOrderAction.OrderRef), bytes2str(pRspInfo.ErrorMsg))

    def OnRtnOrder(self, pOrder):
        self.logger.debug('{}: OnRtnOrder: {}'.format(self.name, pOrder))
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

    def OnRtnTrade(self, pTrade):
        self.logger.debug('{}: OnRtnTrade: {}'.format(self.name, pTrade))
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

    def create_order(self, order):
        self.logger.debug('{}: CreateOrder: {}'.format(self.name, order))
        ins_dict = self._ins_cache.get(order.order_book_id)
        if ins_dict is None:
            order.update_status(
                ORDER_STATUS.REJECTED,
                message='Account is not inited successfully or instrument {} is not trading.'.format(
                    order.order_book_id
                )
            )
            return
        try:
            price_type = ORDER_TYPE_MAPPING[order.type]
        except KeyError:
            order.update_status(
                ORDER_STATUS.REJECTED, message='Order type {} is not supported by gateway.'.format(order.type)
            )
            return

        try:
            position_effect = POSITION_EFFECT_MAPPING[order.position_effect]
        except KeyError:
            order.update_status(
                ORDER_STATUS.REJECTED,
                message='Order position effect {} is not supported by gateway.'.format(order.position_effect)
            )

            return

        req_id = self.req_id
        req = ApiStruct.InputOrder(
            InstrumentID=str2bytes(ins_dict['instrument_id']),
            LimitPrice=float(order.price),
            VolumeTotalOriginal=int(order.quantity),
            OrderPriceType=price_type,
            Direction=SIDE_MAPPING.get(order.side, ''),
            CombOffsetFlag=position_effect,

            OrderRef=str2bytes(str(order.order_id)),
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

    def cancel_order(self, order):
        self.logger.debug('{}: CancelOrder: {}'.format(self.name, order))
        ins_dict = self._ins_cache.get(order.order_book_id)
        if ins_dict is None:
            # TODO: cancal failed
            order.update_status(ORDER_STATUS.REJECTED, message='Instrument {} is not in trading currently'.format(order.order_book_id))
            return None

        req_id = self.req_id
        req = ApiStruct.InputOrderAction(
            InstrumentID=str2bytes(ins_dict['instrument_id']),
            ExchangeID=str2bytes(ins_dict['exchange_id']),
            OrderRef=str2bytes(str(order.order_id)),
            FrontID=int(self._front_id),
            SessionID=int(order.session_id),

            ActionFlag=ApiStruct.AF_Delete,
            BrokerID=str2bytes(self.broker_id),
            InvestorID=str2bytes(self.user_id),

            RequestID=req_id,
        )
        self.ReqOrderAction(req, req_id)
