#!/usr/bin/env python
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
import click

from rqalpha.__main__ import cli

__config__ = {
    "enabled": True,
    "priority": 101,
    "user_id": None,
    "password": None,
    "broker_id": None,
    "md_frontend_url": "tcp://180.168.146.187:10011",
    "trade_frontend_url": "tcp://180.168.146.187:10001",

    "real_init_position": True,
    "real_commission_margin_rate": False,
    "real_trade_time": False,
}


cli_prefix = "mod__ctp__"

cli.commands['run'].params.append(
    click.Option(
        ('--ctp-user-id', cli_prefix + 'user_id'),
        type=click.STRING,
        help="[ctp] uesr_id",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('--ctp-password', cli_prefix + 'password'),
        type=click.STRING,
        help="[ctp] password",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('--ctp-broker-id', cli_prefix + 'broker_id'),
        type=click.STRING,
        help="[ctp] broker_id",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('--ctp-trade-frontend-url', cli_prefix + 'trade_frontend_url'),
        type=click.STRING,
        help="[ctp] trade address",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('--ctp-md-frontend-url', cli_prefix + 'md_frontend_url'),
        type=click.STRING,
        help="[ctp] market data address",
    )
)


def load_mod():
    from .mod import CtpMod
    return CtpMod()
