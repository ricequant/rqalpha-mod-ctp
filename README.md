# RQAlpha-mod-ctp

RQAlpha 对接 CTP 的扩展 Mod。通过启用该 Mod 来实现期货策略的实盘交易。目前本模块仍处在正式发布前的测试阶段，您可以下载参与测试并开 Issue 提交 bug，也欢迎您提交代码，参与开发。

该 Mod 底层引用了 [PyCTP](https://github.com/lovelylain/pyctp), 一些函数的封装和写法参考了 [VN.PY](https://github.com/vnpy/vnpy)。


## 量化社区

[Ricequant](http://www.ricequant.com) 旨在为量化爱好者提供可靠、易用的工具。与此同时我们也搭建了一个量化社区供大家探讨与交流回测框架、策略。欢迎大家前来分享自己对于量化交易的理解。

## 环境要求

该 Mod 支持 python 2.7, 3.4, 3.5，暂时仅支持 Linux，后续会加入 windows 支持。

## 安装

```bash

rqalpha mod install ctp

```

或者将源代码克隆，并在安装目录执行 

```bash

rqalpha mod install -e .

```

## 配置项

您需要在配置项中填入您的 CTP 账号密码等信息，您可以在 simnow 官网 申请实盘模拟账号。
配置项的使用与 RQAlpha 其他 mod 无异

```python

{   
    # CTP 登录信息
    "login": {
        'user_id': None,
        'password': None,
        'broker_id': "9999",
    },
    # 事件相关设置
    "event": {
        # 是否使用默认的 CTP 实时数据源
        "enabled": True,
        # 是否在非交易时间段内触发行情事件
        "all_day": False,
        "address": "tcp://180.168.212.228:41213",
    },
    # 交易相关设置
    "trade": {
        # 是否使用默认的 CTP 交易接口
        "enabled": True,
        "address": "tcp://180.168.146.187:10000",
    },
}

```

## FAQ

FAQ

* 为什么策略在初始化期间停滞了几十秒甚至数分钟？

程序在启动前，需要从 CTP 获取 Instrument 和 Commission 等数据，由于 CTP 控流等原因，向 CTP 发送大量请求会占用很长时间。您可以将 log_level 设置成 verbose 来查看详细的回调函数执行情况。未来可能会考虑开放设置是否全量更新 commission 信息以换取更快的启动速度。


* 为什么我在RQAlpha中查询到的账户、持仓信息与我通过快期等终端查询到的不一致？

本 mod 会尽力将您的账户信息恢复至 RQAlpha 中，但由于计算逻辑的不同，可能会导致各个终端显示的数字有差异，另外您通过其他终端下单交易也有可能导致数据同步的不及时。不过这也有可能是程序bug，如果您发现不一致情况严重，欢迎通过Issue的方式向作者提出。


* 我想要仅仅使用 CTP 的交易/实时行情接口，并配合其他 mod 使用 RQAlpha。

您可以在配置项中将 event 和 trade 部分的 enabled 项设置为 False 来禁用这一部分。


## History

* 0.1.3

    * 对接了 pyctp，实现了 python2 和 python3 下的 RQAlpha 期货实盘。

* 0.1.4 
    
    * 更改了配置项的格式。
    * 拆分了事件和交易部分，用户可以通过配置项将其中一部分禁用。
