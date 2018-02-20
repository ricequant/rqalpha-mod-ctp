import six
import re
from time import sleep
from functools import wraps

from rqalpha.const import ORDER_TYPE, SIDE, POSITION_EFFECT

from .pyctp import ApiStruct


ORDER_TYPE_MAPPING = {
    ORDER_TYPE.MARKET: ApiStruct.OPT_AnyPrice,
    ORDER_TYPE.LIMIT: ApiStruct.OPT_LimitPrice,
}

ORDER_TYPE_REVERSE = {
    ApiStruct.OPT_AnyPrice: ORDER_TYPE.MARKET,
    ApiStruct.OPT_LimitPrice: ORDER_TYPE.LIMIT,
}

SIDE_MAPPING = {
    SIDE.BUY: ApiStruct.D_Buy,
    SIDE.SELL: ApiStruct.D_Sell,
}

SIDE_REVERSE = {
    ApiStruct.D_Buy: SIDE.BUY,
    ApiStruct.D_Sell: SIDE.SELL,
}

POSITION_EFFECT_MAPPING = {
    POSITION_EFFECT.OPEN: ApiStruct.OF_Open,
    POSITION_EFFECT.CLOSE: ApiStruct.OF_Close,
    POSITION_EFFECT.CLOSE_TODAY: ApiStruct.OF_CloseToday,
}


class Status(object):
    ERROR = -1
    NOT_INITIALIZED = 0
    INITIALIZED = 1
    RUNNING = 2


def str2bytes(obj):
    if six.PY2:
        return obj
    else:
        if isinstance(obj, str):
            return obj.encode('GBK')
        elif isinstance(obj, int):
            return str(obj).encode('GBK')
        else:
            return obj


def bytes2str(obj):
    if six.PY2:
        return obj
    else:
        if isinstance(obj, bytes):
            return obj.decode('GBK')
        else:
            return obj


def make_underlying_symbol(id_or_symbol):
    id_or_symbol = bytes2str(id_or_symbol)
    if six.PY2:
        return filter(lambda x: x not in '0123456789 ', id_or_symbol).upper()
    else:
        return ''.join(list(filter(lambda x: x not in '0123456789 ', 'rb1705'))).upper()


def make_order_book_id(symbol):
    symbol = bytes2str(symbol)
    if len(symbol) < 4:
        return None
    if symbol[-4] not in '0123456789':
        order_book_id = symbol[:2] + '1' + symbol[-3:]
    else:
        order_book_id = symbol
    return order_book_id.upper()


def is_future(order_book_id):
    order_book_id = bytes2str(order_book_id)
    if order_book_id is None:
        return False
    return re.match('^[a-zA-Z]+[0-9]+$', order_book_id) is not None


def api_decorator(log=True, check_status=True, raise_error=False):
    def decorator(func):
        @wraps(func)
        def wrapper(api, *args, **kwargs):
            if check_status:
                if api._status < Status.RUNNING:
                    if raise_error:
                        raise RuntimeError('{} was invoked when {} is not ready.'.format(func.__name__, api.name))
                    else:
                        return
            if log:
                try:
                    api.logger.debug('{}: {}: {}'.format(api.name, func.__name__, args[0]))
                except IndexError:
                    api.logger.debug('{}: {}'.format(api.name, func.__name__))
            return func(api, *args, **kwargs)
        return wrapper
    return decorator


def retry_and_find_result(do, done, failed, retry_times=5, timeout=5):
    # return True if success, False if failed, None if timeout
    if retry_times <= 0:
        return
    if done():
        return True
    do()
    for i in range(timeout * 100):
        # print("wait ", i)
        sleep(0.01)
        if done():
            return True
        elif failed():
            return False
    else:
        return retry_and_find_result(do, done, failed, retry_times - 1, timeout * 2)


def query_and_find_result(query, done, failed=lambda: None):
    if done:
        result = retry_and_find_result(query, done, failed)
    else:
        result = retry_and_find_result(query, done=lambda: False, failed=failed, retry_times=1, timeout=2)
        result = True if result is None else result
    return result
