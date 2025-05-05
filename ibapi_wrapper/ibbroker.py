#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015-2020 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
from copy import copy
import datetime
import threading
import uuid
import pytz
import os
import json
import time

# import ib.ext.Order
# import ib.opt as ibopt

import ibapi.order
import ibapi.order_state

from backtrader.feed import DataBase
from backtrader import (TimeFrame, num2date, date2num, BrokerBase,
                        Order, OrderBase, OrderData)
from backtrader.utils.py3 import bytes, bstr, with_metaclass, queue, MAXFLOAT
from backtrader.metabase import MetaParams
from backtrader.comminfo import CommInfoBase
from backtrader.position import Position
from ibapi_wrapper import ibstore
from backtrader.utils import TZLocal
from backtrader.comminfo import CommInfoBase

bytes = bstr  # py2/3 need for ibpy

import logging


class IBOrderState(object):
    # wraps OrderState object and can print it
    _fields = ['status', 'initMargin', 'maintMargin', 'equityWithLoan',
               'commission', 'minCommission', 'maxCommission',
               'commissionCurrency', 'warningText']

    def __init__(self, orderstate):
        for f in self._fields:
            # fname = 'm_' + f
            fname =  f
            setattr(self, fname, getattr(orderstate, fname))

    def __str__(self):
        txt = list()
        txt.append('--- ORDERSTATE BEGIN')
        for f in self._fields:
            # fname = 'm_' + f
            fname = f
            txt.append('{}: {}'.format(f.capitalize(), getattr(self, fname)))
        txt.append('--- ORDERSTATE END')
        return '\n'.join(txt)


class IBOrder(OrderBase, ibapi.order.Order):
    '''Subclasses the IBPy order to provide the minimum extra functionality
    needed to be compatible with the internally defined orders

    Once ``OrderBase`` has processed the parameters, the __init__ method takes
    over to use the parameter values and set the appropriate values in the
    ib.ext.Order.Order object

    Any extra parameters supplied with kwargs are applied directly to the
    ib.ext.Order.Order object, which could be used as follows::

      Example: if the 4 order execution types directly supported by
      ``backtrader`` are not enough, in the case of for example
      *Interactive Brokers* the following could be passed as *kwargs*::

        orderType='LIT', lmtPrice=10.0, auxPrice=9.8

      This would override the settings created by ``backtrader`` and
      generate a ``LIMIT IF TOUCHED`` order with a *touched* price of 9.8
      and a *limit* price of 10.0.

    This would be done almost always from the ``Buy`` and ``Sell`` methods of
    the ``Strategy`` subclass being used in ``Cerebro``
    '''

    def __str__(self):
        '''Get the printout from the base class and add some ib.Order specific
        fields'''
        basetxt = super(IBOrder, self).__str__()
        tojoin = [basetxt]
        tojoin.append('Ref: {}'.format(self.ref))
        tojoin.append('orderId: {}'.format(self.orderId))
        tojoin.append('Action: {}'.format(self.action))
        tojoin.append('Size (ib): {}'.format(self.totalQuantity))
        tojoin.append('Lmt Price: {}'.format(self.lmtPrice))
        tojoin.append('Aux Price: {}'.format(self.auxPrice))
        tojoin.append('OrderType: {}'.format(self.orderType))
        tojoin.append('Tif (Time in Force): {}'.format(self.tif))
        tojoin.append('GoodTillDate: {}'.format(self.goodTillDate))
        return '\n'.join(tojoin)

    # Map backtrader order types to the ib specifics
    _IBOrdTypes = {
        None: bytes('MKT'),  # default
        Order.Market: bytes('MKT'),
        Order.Limit: bytes('LMT'),
        Order.Close: bytes('MOC'),
        Order.Stop: bytes('STP'),
        Order.StopLimit: bytes('STPLMT'),
        Order.StopTrail: bytes('TRAIL'),
        Order.StopTrailLimit: bytes('TRAIL LIMIT'),
    }

    def __init__(self, action, **kwargs):

        # Marker to indicate an openOrder has been seen with
        # PendinCancel/Cancelled which is indication of an upcoming
        # cancellation
        self._willexpire = False

        self.ordtype = self.Buy if action == 'BUY' else self.Sell

        super(IBOrder, self).__init__()
        ibapi.order.Order.__init__(self)  # Invoke 2nd base class

        # Now fill in the specific IB parameters
        self.orderType = self._IBOrdTypes[self.exectype]
        self.permid = 0

        # 'B' or 'S' should be enough
        self.action = bytes(action)

        # Set the prices
        self.lmtPrice = 0.0
        self.auxPrice = 0.0

        if self.exectype == self.Market:  # is it really needed for Market?
            pass
        elif self.exectype == self.Close:  # is it ireally needed for Close?
            pass
        elif self.exectype == self.Limit:
            self.lmtPrice = self.price
        elif self.exectype == self.Stop:
            self.auxPrice = self.price  # stop price / exec is market
        elif self.exectype == self.StopLimit:
            self.lmtPrice = self.pricelimit  # req limit execution
            self.auxPrice = self.price  # trigger price
        elif self.exectype == self.StopTrail:
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.m_trailingPercent = self.trailpercent * 100.0
        elif self.exectype == self.StopTrailLimit:
            self.trailStopPrice = self.price
            # this is the only way i could get limit offset and limit to work together in sync
            # https://groups.io/g/twsapi/topic/trail_limit_orders/34461744
            # https://interactivebrokers.github.io/tws-api/basic_orders.html#trailingstoplimit
            # Important: the 'limit offset' field is set by default in the TWS/IBG settings in v963+. This setting either needs to be changed in the Order Presets, the default value accepted, or the limit price offset sent from the API as in the example below. Not both the 'limit price' and 'limit price offset' fields can be set in TRAIL LIMIT orders.
            self.lmtPriceOffset = abs(self._limitoffset)
            if self.trailamount is not None:
                self.auxPrice = self.trailamount
            elif self.trailpercent is not None:
                # value expected in % format ... multiply 100.0
                self.m_trailingPercent = self.trailpercent * 100.0
                self.trailingPercent = self.m_trailingPercent

        self.totalQuantity = abs(self.size)  # ib takes only positives
        tz = self.data._gettz()

        # self.m_transmit = self.transmit
        if self.parent is not None:
            self.m_parentId = self.parent.orderId

        # Time In Force: DAY, GTC, IOC, GTD
        if self.valid is None:
            tif = 'GTC'  # Good til cancelled
        elif self.valid == 0:
            tif = 'GTD'
            valid = datetime.datetime.combine(
                self.data.datetime.date(), datetime.time(23, 59, 59, 9999))
            tz_dt = tz.localize(valid)
            self.goodTillDate = bytes(tz_dt.strftime('%Y%m%d %H:%M:%S' + ' ' + tz.zone))
        else:
            tif = 'GTD'  # Good til date
            valid = num2date(self.valid)
            tz_dt = pytz.utc.localize(valid).astimezone(tz)
            self.goodTillDate = bytes(tz_dt.strftime('%Y%m%d %H:%M:%S' + ' ' + tz.zone))

        self.tif = bytes(tif)

        # OCA
        self.ocaType = 1  # Cancel all remaining orders with block

        # pass any custom arguments to the order
        for key, value in kwargs.items():
            setattr(self, key, value)

        self.order_state = ibapi.order_state.OrderState()
        self.contract = self.data.tradecontract

        self.create_time = time.time()
        self.update_time = time.time()
        # https://stackoverflow.com/questions/74943249/python-program-that-buys-options-using-ibkr
        # https://groups.io/g/twsapi/topic/the_etradeonly_order/82377975
        self.eTradeOnly = False
        self.firmQuoteOnly = False

    def get_contract(self):
        return self.contract
    
    def get_order_state(self):
        return self.order_state

    def set_order_state(self, state):
        '''
        called after receiving an order state update
        '''
        self.order_state = state

    def submit(self, *args, **kwargs):
        self.order_state.status = 'Submitted'
        return super().submit(*args, **kwargs)


class IBCommInfo(CommInfoBase):
    '''
    Commissions are calculated by ib, but the trades calculations in the
    ```Strategy`` rely on the order carrying a CommInfo object attached for the
    calculation of the operation cost and value.

    These are non-critical informations, but removing them from the trade could
    break existing usage and it is better to provide a CommInfo objet which
    enables those calculations even if with approvimate values.

    The margin calculation is not a known in advance information with IB
    (margin impact can be gotten from OrderState objects) and therefore it is
    left as future exercise to get it'''

    def getvaluesize(self, size, price):
        # In real life the margin approaches the price
        return abs(size) * price

    def getoperationcost(self, size, price):
        '''Returns the needed amount of cash an operation would cost'''
        # Same reasoning as above
        return abs(float(size)) * float(price)


class MetaIBBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaIBBroker, cls).__init__(name, bases, dct)
        ibstore.IBStore.BrokerCls = cls


class IBBroker(with_metaclass(MetaIBBroker, BrokerBase)):
    '''Broker implementation for Interactive Brokers.

    This class maps the orders/positions from Interactive Brokers to the
    internal API of ``backtrader``.

    Notes:

      - ``tradeid`` is not really supported, because the profit and loss are
        taken directly from IB. Because (as expected) calculates it in FIFO
        manner, the pnl is not accurate for the tradeid.

      - Position

        If there is an open position for an asset at the beginning of
        operaitons or orders given by other means change a position, the trades
        calculated in the ``Strategy`` in cerebro will not reflect the reality.

        To avoid this, this broker would have to do its own position
        management which would also allow tradeid with multiple ids (profit and
        loss would also be calculated locally), but could be considered to be
        defeating the purpose of working with a live broker
    '''
    params = ()

    def __init__(self, **kwargs):
        super(IBBroker, self).__init__()

        self.ib = None  # ibstore.IBStore(**kwargs)
        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0

        # see push_ordererror - why rlock is needed
        self._lock_orders = threading.RLock()  # control access
        self.orderbyid = dict()  # orders by order id
        self.loaded_orders = dict()  # orders loaded from file
        self.received_orders = queue.Queue()  # orders received from broker
        self.executions = dict()  # notified executions
        self.ordstatus = collections.defaultdict(dict)
        self.notifs = queue.Queue()  # holds orders which are notified
        self.tonotify = collections.deque()  # hold oids to be notified
        self.broker_orders = queue.Queue()   # hold opened orders
        self.save_path = kwargs.get("save_path", os.path.join(os.path.realpath(os.curdir), "TradeData"))
        self.save_completed_path = os.path.abspath(os.path.join(self.save_path, "Completed"))
        self.save_expire_path = os.path.abspath(os.path.join(self.save_path, "Expired"))

        self.is_requesting_open_orders = False
        self.request_open_orders_count = 0
        self.request_open_orders_time = 0
        self.check_connection_time = 0

        self.init_logger()

    def init_logger(self):
        self.logger = logging.getLogger(__name__)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)
        self.logger.addHandler(stream_handler)
        self.logger.setLevel(logging.INFO)

    def start(self):
        super(IBBroker, self).start()
        self.ib = ibstore.IBStore()
        self.ib.start(broker=self)
        if self.ib.connected():
            self.ib.reqAccountUpdates()
            self.startingcash = self.cash = self.ib.get_acc_cash()
            self.startingvalue = self.value = self.ib.get_acc_value()
        else:
            self.startingcash = self.cash = 0.0
            self.startingvalue = self.value = 0.0
        
        # subscribe position updates
        self.ib.reqPositions()

    def stop(self):
        super(IBBroker, self).stop()
        self.ib.stop()

    def getcash(self):
        # This call cannot block if no answer is available from ib
        self.cash = self.ib.get_acc_cash()
        self.logger.debug(f"get_acc_cash: {self.cash}")
        return self.cash

    def getvalue(self, datas=None):
        self.value = self.ib.get_acc_value()
        self.logger.debug(f"getvalue: {self.value}")
        return self.value

    def getposition(self, data, clone=True):
        position = self.ib.getposition(data.tradecontract, clone=clone)
        self.logger.debug(f"getposition: {position}")
        return position

    def cancel(self, order):
        try:
            o = self.orderbyid[order.orderId]
        except (ValueError, KeyError):
            return  # not found ... not cancellable

        if order.status == Order.Cancelled:  # already cancelled
            return

        self.ib.cancelOrder(order.orderId)

    def orderstatus(self, order):
        try:
            o = self.orderbyid[order.orderId]
        except (ValueError, KeyError):
            o = order

        return o.status

    def submit(self, order):
        order.submit(self)

        # ocoize if needed
        if order.oco is None:  # Generate a UniqueId
            order.ocaGroup = bytes(uuid.uuid4())
        else:
            order.ocaGroup = self.orderbyid[order.oco.orderId].ocaGroup

        self.orderbyid[order.orderId] = order
        self.ib.placeOrder(order.orderId, order.data.tradecontract, order)
        self.notify(order)

        return order

    def getcommissioninfo(self, data):
        self.logger.info("getcommissioninfo()")
        contract = data.tradecontract
        try:
            mult = float(contract.multiplier)
        except (ValueError, TypeError):
            mult = 1.0

        stocklike = contract.secType not in ('FUT', 'OPT', 'FOP',)

        return IBCommInfo(mult=mult, stocklike=stocklike)

    def _makeorder(self, action, owner, data,
                   size, price=None, plimit=None,
                   exectype=None, valid=None,
                   tradeid=0, **kwargs):

        orderId=self.ib.nextOrderId()
        order = IBOrder(action, owner=owner, data=data,
                        size=size, price=price, pricelimit=plimit,
                        exectype=exectype, valid=valid,
                        tradeid=tradeid,
                        clientId=self.ib.clientId,
                        orderId=orderId,
                        **kwargs)

        order.addcomminfo(self.getcommissioninfo(data))
        return order

    def buy(self, owner, data,
            size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0,
            **kwargs):

        order = self._makeorder(
            'BUY',
            owner, data, size, price, plimit, exectype, valid, tradeid,
            **kwargs)

        return self.submit(order)

    def sell(self, owner, data,
             size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0,
             **kwargs):

        order = self._makeorder(
            'SELL',
            owner, data, size, price, plimit, exectype, valid, tradeid,
            **kwargs)

        return self.submit(order)

    def notify(self, order, save=True):
        self.notifs.put(order.clone())
        order.update_time = time.time()
        if save:
            self._save_order(order)

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            pass

        return None

    def _check_connection(self):
        reason = ""
        if self.ib.connected():
            if not self.ib.data_is_lost():
                return
            else:
                self.logger.info("Force discoonect from IB Gateway because data lost")
                if self.ib.force_close_connection():
                    self.logger.info("Wait for 20 seconds to disconnect.")
                    time.sleep(20)
                else:
                    self.logger.info("Cannot disconnect from IB Gateway.")
                    return
                reason = "data lost"
        else:
            reason = "connection lost"

        if self.ib.connected():
            self.logger.error("Cannot reconnect to IB Gateway because the connection is still alive.")
            return

        self.logger.info(f"Try to reconnect to IB Gateway, reason: {reason}")
        if self.ib.reconnect(fromstart=True):
            # if reconnect success, we need to wait for a while to get the orders
            time.sleep(5)
            self.ib.rebuild_after_reconnect()
            self.rebuild_after_reconnect()

    def next(self):
        orders_empty = self.broker_orders.empty()
        now = time.time()
        while not self.broker_orders.empty():
            msg = self.broker_orders.get()
            self.rebuild_iborder_from_open_order(msg)
        if not orders_empty:
            # Because the orders is not empty, we cannot confirm that the order status is right
            # because we discards the status updation when we cannot find the order in self.orderbyid
            # so we need to request the orders again to get the status of each order because there is no way to get the status of each order in the open order callback
            # the only way to get the status of each order is to request the orders from broker
            # if the orders is empty, it means when we receive the order msg, the order id is in self.orderbyid or no other orders
            self.request_broker_orders()
        else:
            # check the broker orders every 5 miniutes
            if self.request_open_orders_time != 0 and now - self.request_open_orders_time > 300:
                self.request_broker_orders()

        if now - self.check_connection_time > 60:
            # if the connection is lost, try to reconnect every 60 seconds
            self._check_connection()
            self.check_connection_time = time.time()
            self.ib.reqCurrentTime()

        self.notifs.put(None)  # mark notificatino boundary

    # Order statuses in msg
    (SUBMITTED, FILLED, CANCELLED, INACTIVE,
     PENDINGSUBMIT, PENDINGCANCEL, PRESUBMITTED, APICANCELLED, APIPENDING) = (
        'Submitted', 'Filled', 'Cancelled', 'Inactive',
         'PendingSubmit', 'PendingCancel', 'PreSubmitted', "ApiCancelled", "ApiPending")

    def push_orderstatus(self, msg):
        # Cancelled and Submitted with Filled = 0 can be pushed immediately
        try:
            with self._lock_orders:
                order = self.orderbyid[msg.orderId]
        except KeyError:
            return  # not found, it was not an order

        order.order_state.status = msg.status
        if msg.status == self.SUBMITTED and msg.filled == 0:
            if order.status == order.Accepted:  # duplicate detection
                return

            order.accept(self)
            self.notify(order)

        elif msg.status == self.CANCELLED or msg.status == self.APICANCELLED:
            # duplicate detection
            if order.status in [order.Cancelled, order.Expired]:
                return

            if order._willexpire:
                # An openOrder has been seen with PendingCancel/Cancelled
                # and this happens when an order expires
                order.expire()
            else:
                # Pure user cancellation happens without an openOrder
                order.cancel()
            self.notify(order)

        elif msg.status == self.PENDINGCANCEL:
            # In theory this message should not be seen according to the docs,
            # but other messages like PENDINGSUBMIT which are similarly
            # described in the docs have been received in the demo
            if order.status == order.Cancelled:  # duplicate detection
                return

            # We do nothing because the situation is handled with the 202 error
            # code if no orderStatus with CANCELLED is seen
            # order.cancel()
            # self.notify(order)

        elif msg.status == self.INACTIVE:
            # This is a tricky one, because the instances seen have led to
            # order rejection in the demo, but according to the docs there may
            # be a number of reasons and it seems like it could be reactivated
            if order.status == order.Rejected:  # duplicate detection
                return

            order.reject(self)
            self.notify(order)

        elif msg.status in [self.SUBMITTED, self.FILLED]:
            # These two are kept inside the order until execdetails and
            # commission are all in place - commission is the last to come
            self.ordstatus[msg.orderId][msg.filled] = msg

        elif msg.status in [self.PENDINGSUBMIT, self.PRESUBMITTED, self.APIPENDING]:
            # According to the docs, these statuses can only be set by the
            # programmer but the demo account sent it back at random times with
            # "filled"
            if msg.filled:
                self.ordstatus[msg.orderId][msg.filled] = msg
        else:  # Unknown status ...
            self.logger.warning(f"Unknown status,  orderId: {msg.orderId}, order status: {msg.status}")
            pass

    def rebuild_order(self, order_data):
        dataname = order_data["dataname"]
        stratname = order_data["stratname"]
        data = self.cerebro.datasbyname[dataname]
        # flat_strats = list(itertools.chain.from_iterable(self.cerebro.strats))
        # strats_by_name =  {k[0].__name__:k for k in flat_strats}
        # owner = strats_by_name[stratname][0] # the strats tuple is (class, empty, params)
        strats_by_name =  {k.__class__.__name__:k for k in self.cerebro.runningstrats}
        owner = strats_by_name[stratname]
        size = order_data["size"]
        price = order_data["price"]
        pricelimit = order_data["pricelimit"]
        action = order_data["action"]
        ibstatus = order_data["ibstatus"]
        order_type = order_data["order_type"]
        tradeid = order_data["tradeid"]
        client_id = order_data["client_id"]
        order_id = order_data["order_id"]
        valid = order_data["valid"]
        good_till_date = order_data["good_till_date"]
        perm_id = order_data["perm_id"]
        tif = order_data["tif"]
        status = order_data["status"]
        exec_type = order_data["exec_type"]
        create_time = order_data["create_time"]
        update_time = order_data["update_time"]
        uuid = order_data["uuid"]

        if data is None:
            self.logger.warning(f"We cannot find dataname {dataname} in cerebro, {order_id} {action} {ibstatus}, maybe changed the data config")
            return None

        ib_order = IBOrder(simulated=True, action=action, owner=owner, data=data, 
                           size=size, price=price, pricelimit=pricelimit,
                           exectype=exec_type, valid=None,
                           tradeid=tradeid, 
                           clientId=client_id, orderId=order_id)

        ib_order.order_state.status = ibstatus
        ib_order.valid = valid
        ib_order.goodTillDate = good_till_date
        ib_order.permId = perm_id
        ib_order.tif = bytes(tif)
        ib_order.status = status
        ib_order.orderType = order_type
        ib_order.p.simulated = False
        ib_order.create_time = create_time
        ib_order.update_time = update_time
        ib_order.uuid = uuid

        return ib_order

    def rebuild_iborder_from_open_order(self, msg):
        order_id = msg.orderId
        contract = msg.contract
        order = msg.order
        order_status = msg.orderState.status
        client_id = order.clientId

        # check the order data
        if client_id != self.ib.clientId:
            return

        order_data = self._load_order(contract.symbol, client_id, order_id)
        if order_data == None:
            self.logger.error(f"ERROR: Cannot load order data from file, {order_id} {contract.symbol} {order_status}")
            return
        if order_data["order_id"] != order_id:
            self.logger.error(f"ERROR: order data wrong, {order_data['order_id']} vs {order_id} {contract.symbol} {order_status}")
            return

        ib_order = self.rebuild_order(order_data)
        if ib_order is None:
            return

        # set status and notify the order
        with self._lock_orders:
            self.orderbyid[order_id] = ib_order

        self.notify(ib_order, False)

    def check_order_existance(self):
        received_orders = set()
        while not self.received_orders.empty():
            received_orders.add(self.received_orders.get())

        # delete the orders that is expire or not exists
        need_delete = []
        for orderid, orderdata in self.loaded_orders.items():
            if orderid not in received_orders:
                orderdata["not_exists_cnt"] += 1
                if orderdata["not_exists_cnt"] > 3:
                    need_delete.append(orderid)

        for orderid in need_delete:
            # move the order to expire folder
            filename = self.loaded_orders[orderid]["filename"]
            if not os.path.exists(self.save_expire_path):
                os.makedirs(self.save_expire_path)

            index = 1
            while True:
                dest_name = f"{orderdata['symbol']}_{orderdata['client_id']}_{orderdata['order_id']}_{index}.json"
                if not os.path.exists(os.path.join(self.save_expire_path, dest_name)):
                    break
                else:
                    index += 1
            dest_path = os.path.abspath(os.path.join(self.save_expire_path, dest_name))
            src_path = os.path.join(self.save_path, filename)
            if not os.path.exists(src_path):
                self.logger.info(f"Cannot find the order file {src_path} for moving to expire folder {dest_path}")
            else:
                os.rename(src_path, os.path.join(self.save_expire_path, dest_path))
                self.logger.info(f"Move the expired order {filename} to expire folder")
            if orderid in self.orderbyid:
                self.orderbyid.pop(orderid)
            self.loaded_orders.pop(orderid)

    def push_execution(self, ex):
        self.executions[ex.execId] = ex

    def push_commissionreport(self, cr):
        with self._lock_orders:
            try:
                ex = self.executions.pop(cr.execId)
                oid = ex.orderId
                order = self.orderbyid[oid]
                ostatus = self.ordstatus[oid].pop(ex.cumQty)

                position = self.getposition(order.data, clone=False)
                pprice_orig = position.price
                size = ex.shares if ex.side[0] == 'B' else -ex.shares
                price = ex.price
                # use pseudoupdate and let the updateportfolio do the real update?
                psize, pprice, opened, closed = position.update(float(size), price)

                # split commission between closed and opened
                comm = cr.commission
                closedcomm = comm * float(closed) / float(size)
                openedcomm = comm - closedcomm

                comminfo = order.comminfo
                closedvalue = comminfo.getoperationcost(closed, pprice_orig)
                openedvalue = comminfo.getoperationcost(opened, price)

                # default in m_pnl is MAXFLOAT
                pnl = cr.realizedPNL if closed else 0.0

                # The internal broker calc should yield the same result
                # pnl = comminfo.profitandloss(-closed, pprice_orig, price)

                # Use the actual time provided by the execution object
                # The report from TWS is in actual local time, not the data's tz
                #dt = date2num(datetime.strptime(ex.time, '%Y%m%d  %H:%M:%S'))
                dt_array = [] if ex.time == None else list(filter(bool,ex.time.split(" ")))
                if dt_array and len(dt_array) > 1:
                  ex_time = " ".join(dt_array)
                  dt = date2num(datetime.datetime.strptime(ex_time, '%Y%m%d %H:%M:%S'))
                else:
                  dt = date2num(datetime.datetime.strptime(ex.time, '%Y%m%d %H:%M:%S %A'))

                # Need to simulate a margin, but it plays no role, because it is
                # controlled by a real broker. Let's set the price of the item
                margin = order.data.close[0]

                order.execute(dt, float(size), price,
                            float(closed), closedvalue, closedcomm,
                            opened, openedvalue, openedcomm,
                            margin, pnl,
                            float(psize), pprice)

                if ostatus.status == self.FILLED:
                    order.completed()
                    self.ordstatus.pop(oid)  # nothing left to be reported
                else:
                    order.partial()

                if oid not in self.tonotify:  # Lock needed
                    self.tonotify.append(oid)
            except Exception as e:
                self.logger.exception(f"Exception: {e}")

    def push_portupdate(self):
        # If the IBStore receives a Portfolio update, then this method will be
        # indicated. If the execution of an order is split in serveral lots,
        # updatePortfolio messages will be intermixed, which is used as a
        # signal to indicate that the strategy can be notified
        with self._lock_orders:
            while self.tonotify:
                oid = self.tonotify.popleft()
                order = self.orderbyid[oid]
                self.notify(order)

    # this line causes deadlock - if self.request_broker_orders is called within our lock, it'll hang
    def push_ordererror(self, msg):
        with self._lock_orders:
            try:
                order = self.orderbyid[msg.id]
            except (KeyError, AttributeError):
                self.request_broker_orders()
                return  # no order or no id in error

            if msg.errorCode == 202:
                if not order.alive():
                    return
                order.cancel()

            elif msg.errorCode == 201:  # rejected
                if order.status == order.Rejected:
                    return
                order.reject()

            else:
                order.reject()  # default for all other cases

            self.notify(order)

        self.request_broker_orders()

    def push_openorder(self, msg=None):
        '''
        In this callback, we only need to recreate the non-existent orders because the existing orders will have their status updated in other callbacks.
        If this is the initial stage, this can be ignored for now, as IBAPI might have downloaded all open orders when connecting.
        We will continue to request orders later. Since there is a lot of data not yet ready at the initial stage, we can ignore this stage for now.
        '''
        if msg == None:
            self.logger.info("Receive all opened orders request from broker")
            # all open order push finished
            # self.broker_orders.put(None)
            need_request = False
            with self._lock_orders:
                self.is_requesting_open_orders = False
                if self.request_open_orders_count > 0:
                    need_request = True
            if need_request:
                self.request_broker_orders()
            else:
                self.check_order_existance()
            return

        self.received_orders.put(msg.orderId)

        has_order = False
        with self._lock_orders:
            has_order = True if msg.orderId in self.orderbyid else False

        if has_order:
            # In this callback, we only need to recreate the non-existent orders
            # because the existing orders will have their status updated in other callbacks.
            # just return here
            return
        else:
            self.broker_orders.put(msg)
            return

    def push_completedorders(self, msg):
        # The same as the open order, we only need to recreate the non-existent orders
        self.push_openorder(msg)

    def _load_order(self, symbol, client_id, order_id):
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)
        if not os.path.exists(self.save_completed_path):
            os.makedirs(self.save_completed_path)
        filename = f"{symbol}_{client_id}_{order_id}.json"
        path = os.path.join(self.save_path, filename)
        if not os.path.exists(path):
            return None
        order_data = json.load(open(path, "r"))
        if order_data["valid"] is None:
            order_data["valid"] = None
        else:
            time_str = order_data["valid"]
            datetime_part, timezone_part = time_str.rsplit(' ', 1)
            naive_time = datetime.datetime.strptime(datetime_part, "%Y-%m-%d %H:%M:%S")
            timezone = pytz.timezone(timezone_part)
            order_data["valid"] = timezone.localize(naive_time)
        order_data["symbol"] = symbol
        order_data["filename"] = filename
        order_data["not_exists_cnt"] = 0
        return order_data

    def _save_order(self, order):
        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)
        if not os.path.exists(self.save_completed_path):
            os.makedirs(self.save_completed_path)
        order_id = order.orderId
        save_data = {
            "order_id": order_id,
            "client_id": order.clientId,
            "dataname": order.data._name,
            "stratname": order.owner.__class__.__name__,
            "size": order.totalQuantity,
            "price": order.lmtPrice,
            "pricelimit": order.auxPrice,
            "perm_id": order.permId,
            "action": order.action,
            "order_type": order.orderType,
            "exec_type": order.exectype,
            "tif": order.tif,
            "good_till_date": order.goodTillDate,
            "status": order.status,
            "ibstatus": order.order_state.status,
            "valid": order.valid.strftime("%Y-%m-%d %H:%M:%S %Z%z") if order.valid else None,
            "tradeid": order.tradeid,
            "symbol": order.contract.symbol,
            "con_id": order.contract.conId,
            "sec_type": order.contract.secType,
            "currency": order.contract.currency,
            "exchange": order.contract.exchange,
            "create_time": order.create_time,
            "update_time": order.update_time,
            # "uuid": order.uuid,
            "uuid": order_id,
        }
        filename = f"{order.contract.symbol}_{order.clientId}_{order_id}.json"
        save_path = os.path.join(self.save_path, filename)
        json.dump(save_data, open(save_path, "w"))

        self.check_completed_orders(save_data)

    def check_expired_orders(self, order_data):
        tif = order_data.get("tif", None)
        if tif == "GTD":
            date_string = order_data.get("good_till_date", None)
            tz_string = date_string.split(" ")[-1]
            date_string = date_string[0:date_string.rfind(" ")]
            end_dt = datetime.datetime.strptime(date_string, "%Y%m%d %H:%M:%S")
            now_dt = datetime.datetime.now(pytz.timezone(tz_string))
            end_dt = pytz.timezone(tz_string).localize(end_dt)
            if end_dt >= now_dt:
                return False
        else:
            return False

        src_filename = f"{order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}.json"
        index = 1
        while True:
            filename = f"{order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}_{index}.json"
            if not os.path.exists(os.path.join(self.save_expire_path, filename)):
                break
            else:
                index += 1
        dest_path = os.path.abspath(os.path.join(self.save_expire_path, filename))
        # move
        os.rename(os.path.join(self.save_path, src_filename), dest_path)
        self.logger.info(f"Order({order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}) move to Expired directory.")
        return True

    def check_completed_orders(self, order_data):
        if order_data["ibstatus"] not in (self.FILLED, self.CANCELLED, self.INACTIVE, self.APICANCELLED):
            return

        src_filename = f"{order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}.json"
        index = 1
        while True:
            filename = f"{order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}_{index}.json"
            if not os.path.exists(os.path.join(self.save_completed_path, filename)):
                break
            else:
                index += 1
        dest_path = os.path.abspath(os.path.join(self.save_completed_path, filename))
        src_file = os.path.join(self.save_path, src_filename)
        # move
        if os.path.exists(src_file):
            os.rename(src_file, dest_path)
            self.logger.info(f"Order({order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}) move to Completed directory.")
        else:
            self.logger.info(f"Order({order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}) Cannot move to Completed directory.")

    def load_orders(self):
        '''
        load all opened orders from file and insert into order list
        and request the broker for all opened orders to determine the status of each order
        '''
        self.logger.info("Start load local orders")
        orders_dir = os.path.realpath(self.save_path)
        for file in os.listdir(orders_dir):
            if file.endswith(".json"):
                # filename is like: MSFT_1_1.json
                name_list = file.split(".")[0].split("_")
                symbol = name_list[0]
                client_id = int(name_list[1])
                order_id = name_list[2]
                if client_id != self.ib.clientId:
                    continue
                order_data = self._load_order(symbol, client_id, order_id)
                self.loaded_orders[int(order_id)] = order_data

        for order_data in self.loaded_orders.values():
            if order_data["ibstatus"] in (self.FILLED, self.CANCELLED, self.INACTIVE, self.APICANCELLED):
                # the order is already completed, no need to rebuild
                self.logger.info(f"Order({order_data['symbol']}_{order_data['client_id']}_{order_data['order_id']}) is already completed, no need to rebuild.")
                # move the file to another directory
                self.check_completed_orders(order_data)
                continue
            else:
                if self.check_expired_orders(order_data):
                    continue
            ib_order = self.rebuild_order(order_data)
            if ib_order is None:
                continue
            if ib_order.orderId not in self.orderbyid:
                self.orderbyid[ib_order.orderId] = ib_order
                self.notify(ib_order)

        self.logger.info("End load local orders")

        # request all opened orders to update the order status
        self.request_broker_orders()

    def request_broker_orders(self):
        '''
        request all opened orders from broker
        '''
        with self._lock_orders:
            if self.is_requesting_open_orders:
                self.request_open_orders_count = 1
                return

            self.is_requesting_open_orders = True
        self.ib.reqCompletedOrders()
        self.ib.reqOpenOrders()
        self.request_open_orders_count = 0
        self.request_open_orders_time = time.time()
        self.received_orders.queue.clear()
        self.logger.info("Start request open orders and completed orders from broker")

    def rebuild_after_reconnect(self):
        self.request_broker_orders()

    def get_symbol_orders(self, symbol):
        order_list = []
        for order in self.orderbyid.values():
            if order.contract.symbol == symbol:
                order_list.append(order)
        return order_list
