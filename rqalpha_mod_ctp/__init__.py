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

    "user_id": None,
    "password": None,
    "broker_id": None,
    "td_addr": 'tcp://180.168.146.187:10000',
    "md_addr": 'tcp://180.168.212.228:41213',
    "all_day": False,
}


cli_prefix = "mod__ctp__"

cli.commands['run'].params.append(
    click.Option(
        ('-ctpu', '--ctp-user-id', cli_prefix + 'user_id'),
        type=click.STRING,
        help="[ctp] uesr_id",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('-ctpp', '--ctp-password', cli_prefix + 'password'),
        type=click.STRING,
        help="[ctp] password",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('-ctpb', '--ctp-broker-id', cli_prefix + 'broker_id'),
        type=click.STRING,
        help="[ctp] broker_id",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('-ctpta', '--ctp-td-addr', cli_prefix + 'td_addr'),
        type=click.STRING,
        help="[ctp] trade address",
    )
)

cli.commands['run'].params.append(
    click.Option(
        ('-ctpma', '--ctp-md-addr', cli_prefix + 'md_addr'),
        type=click.STRING,
        help="[ctp] market data address",
    )
)


cli.commands['run'].params.append(
    click.Option(
        ('-ctpad', '--ctp-all-day', cli_prefix + 'all_day'),
        type=click.BOOL,
        help="[ctp] run strategy all day",
    )
)


def load_mod():
    from .mod import CtpMod
    return CtpMod()
