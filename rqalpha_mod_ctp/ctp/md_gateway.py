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
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty

from rqalpha.utils.logger import system_log
from rqalpha.environment import Environment
from rqalpha.events import EVENT

from .api import CtpMdApi


class MdGateway(object):
    def __init__(self, env, retry_times=5, retry_interval=1):
        self._env = env

        self._md_api = None

        self._retry_times = retry_times
        self._retry_interval = retry_interval

        self._snapshot_cache = {}
        self._tick_que = Queue()
        self.subscribed = []

    def connect(self, user_id, password, broker_id, md_address):
        self._md_api = CtpMdApi(self, user_id, password, broker_id, md_address)

        for i in range(self._retry_times):
            self._md_api.connect()
            sleep(self._retry_interval * (i+1))
            if self._md_api.logged_in:
                self.on_log('CTP 行情服务器登录成功')
                break
        else:
            raise RuntimeError('CTP 行情服务器连接或登录超时')

        self._md_api.subscribe([ins_dict.instrument_id for ins_dict in Environment.get_ins_dict().values()])

        self.on_log('数据同步完成。')

        self._env.event_bus.add_listener(EVENT.POST_UNIVERSE_CHANGED, self.on_universe_changed)

    def get_tick(self):
        while True:
            try:
                return self._tick_que.get(block=True, timeout=1)
            except Empty:
                self.on_debug('Get tick timeout.')

    def exit(self):
        self._md_api.close()

    @property
    def snapshot(self):
        return self._snapshot_cache

    def on_tick(self, tick_dict):
        if tick_dict.order_book_id in self.subscribed:
            self._tick_que.put(tick_dict)
        self._snapshot_cache[tick_dict.order_book_id] = tick_dict

    def on_universe_changed(self, event):
        self.subscribed = event.universe

    @staticmethod
    def on_debug(debug):
        system_log.debug(debug)

    @staticmethod
    def on_log(log):
        system_log.info(log)

    @staticmethod
    def on_err(error, func_name):
        system_log.error('CTP 错误，错误代码：%s，错误信息：%s' % (str(error.ErrorID), error.ErrorMsg.decode('GBK')))
