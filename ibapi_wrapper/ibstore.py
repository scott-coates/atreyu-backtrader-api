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
import itertools
import random
import threading
import time
import math

from backtrader import TimeFrame, Position
from backtrader.metabase import MetaParams  # type: ignore
from backtrader.utils.py3 import bytes, bstr, queue, with_metaclass, long  # type: ignore
from backtrader.utils import AutoDict, TZLocal  # type: ignore

# import official ibapi
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import TickAttrib
from enum import Enum, auto
import logging
import pytz
import tzlocal

bytes = bstr  # py2/3 need for ibpy

store_logger = logging.getLogger(__name__)
store_stream_handler = logging.StreamHandler()
store_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
store_stream_handler = logging.StreamHandler()
store_stream_handler.setFormatter(store_formatter)
store_stream_handler.setLevel(logging.INFO)
store_logger.addHandler(store_stream_handler)
store_logger.setLevel(logging.INFO)


class ErrorMsg(object):
    def __init__(self, reqId, errorCode, errorString, advancedOrderRejectJson):
        self.reqId = reqId
        self.errorCode = errorCode
        self.errorString = errorString
        self.advancedOrderRejectJson = advancedOrderRejectJson
        self.vars = vars()
        del self.vars['self']

    def __str__(self):
        return f'{self.vars}'


class OpenOrderMsg(object):
    def __init__(self, orderId, contract, order, orderState):
        self.orderId = orderId
        self.contract = contract
        self.order = order
        self.orderState = orderState
        self.vars = vars()
        del self.vars['self']

    def __str__(self):
        return f'{self.vars}'


class OrderStatusMsg(object):
    def __init__(self, orderId , status, filled,
                    remaining, avgFillPrice, permId,
                    parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        self.orderId = orderId
        self.status = status
        self.filled = filled
        self.remaining = remaining
        self.avgFillPrice = avgFillPrice
        self.permId = permId
        self.parentId = parentId
        self.lastFillPrice = lastFillPrice
        self.clientId = clientId
        self.whyHeld = whyHeld
        self.mktCapPrice = mktCapPrice
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'

# https://interactivebrokers.github.io/tws-api/tick_types.html
class TickFieldEnum(Enum):
    '''Enum for tick fields
    '''
    BID_SIZE = 0
    BID_PRICE = auto()
    ASK_PRICE = auto()
    ASK_SIZE = auto()
    LAST_PRICE = auto()
    LAST_SIZE = auto()
    HIGH = auto()
    LOW = auto()
    VOLUME = auto()
    CLOSE = auto()
    BID_OPTION_COMPUTATION = auto()
    ASK_OPTION_COMPUTATION = auto()
    LAST_OPTION_COMPUTATION = auto()
    MODEL_OPTION = auto()
    OPEN = auto()
    LOW_13_WEEK = auto()
    HIGH_13_WEEK = auto()
    LOW_26_WEEK = auto()
    HIGH_26_WEEK = auto()
    LOW_52_WEEK = auto()
    HIGH_52_WEEK = auto()
    AVG_VOLUME = auto()
    OPEN_INTEREST = auto()
    OPTION_HISTORICAL_VOL = auto()
    OPTION_IMPLIED_VOL = auto()
    OPTION_BID_EXCH = auto()
    OPTION_ASK_EXCH = auto()
    OPTION_CALL_OPEN_INTEREST = auto()
    OPTION_PUT_OPEN_INTEREST = auto()
    OPTION_CALL_VOLUME = auto()
    OPTION_PUT_VOLUME = auto()
    INDEX_FUTURE_PREMIUM = auto()
    BID_EXCH = auto()
    ASK_EXCH = auto()
    AUCTION_VOLUME = auto()
    AUCTION_PRICE = auto()
    AUCTION_IMBALANCE = auto()
    MARK_PRICE = auto()
    BID_EFP_COMPUTATION = auto()
    ASK_EFP_COMPUTATION = auto()
    LAST_EFP_COMPUTATION = auto()
    OPEN_EFP_COMPUTATION = auto()
    HIGH_EFP_COMPUTATION = auto()
    LOW_EFP_COMPUTATION = auto()
    CLOSE_EFP_COMPUTATION = auto()
    LAST_TIMESTAMP = auto()
    SHORTABLE = auto()
    FUNDAMENTAL_RATIOS = auto()
    RT_VOLUME = auto()
    HALTED = auto()
    BID_YIELD = auto()
    ASK_YIELD = auto()
    LAST_YIELD = auto()
    CUST_OPTION_COMPUTATION = auto()
    TRADE_COUNT = auto()
    TRADE_RATE = auto()
    VOLUME_RATE = auto()
    LAST_RTH_TRADE = auto()
    RT_HISTORICAL_VOL = auto()
    IB_DIVIDENDS = auto()
    BOND_FACTOR_MULTIPLIER = auto()
    REGULATORY_IMBALANCE = auto()
    NEWS_TICK = auto()
    SHORT_TERM_VOLUME_3_MIN = auto()
    SHORT_TERM_VOLUME_5_MIN = auto()
    SHORT_TERM_VOLUME_10_MIN = auto()
    DELAYED_BID = auto()
    DELAYED_ASK = auto()
    DELAYED_LAST = auto()
    DELAYED_BID_SIZE = auto()
    DELAYED_ASK_SIZE = auto()
    DELAYED_LAST_SIZE = auto()
    DELAYED_HIGH = auto()
    DELAYED_LOW = auto()
    DELAYED_VOLUME = auto()
    DELAYED_CLOSE = auto()
    DELAYED_OPEN = auto()
    RT_TRD_VOLUME = auto()
    CREDITMAN_MARK_PRICE = auto()
    CREDITMAN_SLOW_MARK_PRICE = auto()
    DELAYED_BID_OPTION_COMPUTATION = auto()
    DELAYED_ASK_OPTION_COMPUTATION = auto()
    DELAYED_LAST_OPTION_COMPUTATION = auto()
    DELAYED_MODEL_OPTION = auto()
    LAST_EXCH = auto()
    LAST_REG_TIME = auto()
    FUTURES_OPEN_INTEREST = auto()
    AVG_OPT_VOLUME = auto()
    DELAYED_LAST_TIMESTAMP = auto()
    SHORTABLE_SHARES = auto()
    DELAYED_HALTED = auto()

class RTTickData(object):
    def __init__(self, field, value, additional=None):
        self.valid = False
        self.field = None
        self.value = None
        self.datetime = None
        self.additional = None

        try:
            self.field = TickFieldEnum(field).name
            self.value = value
            self.additional = additional
            self.datetime = pytz.timezone(tzlocal.get_localzone_name()).localize(datetime.datetime.now())
            self.valid = value != -1.0
        except ValueError:
            store_logger.error(f"Unknown tick field: {field}")
            self.valid = False
            self.field = None
            self.value = None
            self.additional = None
            self.datetime = None

        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'

    def is_valid(self):
        return self.valid

class RTBar(object):
    '''Set realtimeBar object
    '''
    def __init__(self, reqId, time, open_, high, low, close, volume, wap, count):
        self.reqId = reqId
        self.time = time
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.wap = wap
        self.count = count
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'


class HistBar(object):
    '''Set historicalBar object
    '''
    def __init__(self, reqId, bar):
        self.reqId = reqId
        self.date = bar.date
        self.open = bar.open
        self.high = bar.high
        self.low = bar.low
        self.close = bar.close
        self.volume = bar.volume
        self.wap = bar.wap
        self.count = bar.barCount
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'


class HistTick(object):
    '''Set historicalTick object: 'MIDPOINT', 'BID_ASK', 'TRADES' 
    '''
    def __init__(self, tick, dataType):
        self.date = datetime.datetime.utcfromtimestamp(tick.time)
        self.tickType = tick.tickType if hasattr(tick, 'tickType') else int(0)
        self.dataType = dataType
        if dataType == 'RT_TICK_MIDPOINT':
            self.price = tick.price
        elif dataType == 'RT_TICK_LAST':
            self.price = tick.price
            self.size = float(tick.size)
            self.unreported = tick.tickAttribLast.unreported
            self.pastlimit = tick.tickAttribLast.pastLimit
        elif dataType == 'RT_TICK_BID_ASK':
            self.bidPrice = tick.priceBid
            self.askPrice = tick.priceAsk
            self.bidSize = float(tick.sizeBid)
            self.askSize = float(tick.sizeAsk)

        # self.exchange = tick.exchange
        # self.specialconditions = tick.tickAttribLast.specialConditions
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'


class RTTickLast(object):
    '''Set realtimeTick object: 'TRADES' 
    '''
    def __init__(self, tickType, time, price, size, tickAtrribLast, exchange, specialConditions):
        self.dataType = "RT_TICK_LAST"
        self.datetime = datetime.datetime.utcfromtimestamp(time)
        # self.tickType = TickTypeEnum.to_str(tickType)
        self.tickType = tickType
        self.price = price
        self.size = float(size)
        self.pastlimit = tickAtrribLast.pastLimit
        self.unreported = tickAtrribLast.unreported
        # self.exchange = exchange
        # self.specialConditions = specialConditions
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'


class RTTickBidAsk(object):
    '''Set realtimeTick object: 'MIDPOINT', 'BID_ASK', 'TRADES' 
    '''
    def __init__(self, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
        self.dataType = "RT_TICK_BID_ASK"
        self.datetime = datetime.datetime.utcfromtimestamp(time)
        self.bidPrice = bidPrice
        self.askPrice = askPrice
        self.bidSize = float(bidSize)
        self.askSize = float(askSize)
        self.bidPastLow = tickAttribBidAsk.bidPastLow
        self.askPastHigh = tickAttribBidAsk.askPastHigh
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'


class RTTickMidPoint(object):
    '''Set realtimeTick object: 'MIDPOINT'
    '''
    def __init__(self, time, midPoint):
        self.dataType = "RT_TICK_MIDPOINT"
        self.datetime = datetime.datetime.utcfromtimestamp(time)
        self.midPoint = midPoint
        self.vars = vars()

    def __str__(self):
        return f'{self.vars}'


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


def logibmsg(fn):
    def logmsg_decorator(self, *args, **kwargs):
        try:
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)
            store_logger.debug(f"Calling {fn.__name__}({signature})")
            return fn(self, *args, **kwargs)
        except Exception as e:
            store_logger.exception(f"Exception raised in {fn.__name__}. exception: {str(e)}")
            raise e

    return logmsg_decorator


class IBApi(EWrapper, EClient):
    def __init__(self, cb, _debug):
        EClient.__init__(self, self)
        EWrapper.__init__(self)
        self.cb = cb
        self._debug = _debug

    @logibmsg
    def currentTime(self, time):
        """ Server's current time. This method will receive IB server's system
        time resulting after the invokation of reqCurrentTime. """
        self.cb.currentTime(time)

    @logibmsg
    def updateAccountTime(self, timeStamp):
        self.cb.updateAccountTime(timeStamp)

    @logibmsg
    def nextValidId(self, orderId):
        """ Receives next valid order id."""
        store_logger.debug(f"nextValidId: {orderId}")
        self.cb.nextValidId(orderId)

    @logibmsg
    def connectAck(self):
        """ callback signifying completion of successful connection """
        self.cb.connectAck()

    @logibmsg
    def connectionClosed(self):
        """This function is called when TWS closes the sockets
        connection with the ActiveX control, or when TWS is shut down."""
        store_logger.debug(f"connectionClosed")
        self.cb.connectionClosed()

    @logibmsg
    def managedAccounts(self, accountsList):
        """Receives a comma-separated string with the managed account ids."""
        self.cb.managedAccounts(accountsList)

    @logibmsg
    def accountDownloadEnd(self, accountName):
        """This is called after a batch updateAccountValue() and
        updatePortfolio() is sent."""
        self.cb.accountDownloadEnd(accountName)

    @logibmsg
    def updateAccountValue(self, key, val, currency, accountName):
        """ This function is called only when ReqAccountUpdates on
        EEClientSocket object has been called. """
        self.cb.updateAccountValue(key, val, currency, accountName)

    @logibmsg
    def updatePortfolio(self, contract, position,
                        marketPrice, marketValue,
                        averageCost, unrealizedPNL,
                        realizedPNL, accountName):
        """This function is called only when reqAccountUpdates on
        EEClientSocket object has been called."""
        self.cb.updatePortfolio(contract, position,
                                    marketPrice, marketValue,
                                    averageCost, unrealizedPNL,
                                    realizedPNL, accountName)

    @logibmsg
    def contractDetails(self, reqId, contractDetails):
        """Receives the full contract's definitions. This method will return all
        contracts matching the requested via EEClientSocket::reqContractDetails.
        For example, one can obtain the whole option chain with it."""
        self.cb.contractDetails(reqId, contractDetails)

    @logibmsg
    def contractDetailsEnd(self, reqId):
        """This function is called once all contract details for a given
        request are received. This helps to define the end of an option
        chain."""
        self.cb.contractDetailsEnd(reqId)

    @logibmsg
    def openOrder(self, orderId, contract, order, orderState):
        """This function is called to feed in open orders.

        orderID: OrderId - The order ID assigned by TWS. Use to cancel or
            update TWS order.
        contract: Contract - The Contract class attributes describe the contract.
        order: Order - The Order class gives the details of the open order.
        orderState: OrderState - The orderState class includes attributes Used
            for both pre and post trade margin and commission data."""
        self.cb.openOrder(OpenOrderMsg(orderId, contract, order, orderState))

    @logibmsg
    def completedOrders(self, contract, order, orderState):
        self.cb.completedOrders(OpenOrderMsg(order.orderId, contract, order, orderState))

    @logibmsg
    def openOrderEnd(self):
        """This is called at the end of a given request for open orders."""
        self.cb.openOrderEnd()

    @logibmsg
    def orderStatus(self, orderId , status, filled,
                    remaining, avgFillPrice, permId,
                    parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        """This event is called whenever the status of an order changes. It is
        also fired after reconnecting to TWS if the client has any open orders.

        orderId: OrderId - The order ID that was specified previously in the
            call to placeOrder()
        status:str - The order status. Possible values include:
            PendingSubmit - indicates that you have transmitted the order, but have not  yet received confirmation that it has been accepted by the order destination. NOTE: This order status is not sent by TWS and should be explicitly set by the API developer when an order is submitted.
            PendingCancel - indicates that you have sent a request to cancel the order but have not yet received cancel confirmation from the order destination. At this point, your order is not confirmed canceled. You may still receive an execution while your cancellation request is pending. NOTE: This order status is not sent by TWS and should be explicitly set by the API developer when an order is canceled.
            PreSubmitted - indicates that a simulated order type has been accepted by the IB system and that this order has yet to be elected. The order is held in the IB system until the election criteria are met. At that time the order is transmitted to the order destination as specified.
            Submitted - indicates that your order has been accepted at the order destination and is working.
            Cancelled - indicates that the balance of your order has been confirmed canceled by the IB system. This could occur unexpectedly when IB or the destination has rejected your order.
            Filled - indicates that the order has been completely filled.
            Inactive - indicates that the order has been accepted by the system (simulated orders) or an exchange (native orders) but that currently the order is inactive due to system, exchange or other issues.
        filled:int - Specifies the number of shares that have been executed.
            For more information about partial fills, see Order Status for Partial Fills.
        remaining:int -   Specifies the number of shares still outstanding.
        avgFillPrice:float - The average price of the shares that have been executed. This parameter is valid only if the filled parameter value is greater than zero. Otherwise, the price parameter will be zero.
        permId:int -  The TWS id used to identify orders. Remains the same over TWS sessions.
        parentId:int - The order ID of the parent order, used for bracket and auto trailing stop orders.
        lastFilledPrice:float - The last price of the shares that have been executed. This parameter is valid only if the filled parameter value is greater than zero. Otherwise, the price parameter will be zero.
        clientId:int - The ID of the client (or TWS) that placed the order. Note that TWS orders have a fixed clientId and orderId of 0 that distinguishes them from API orders.
        whyHeld:str - This field is used to identify an order held when TWS is trying to locate shares for a short sell. The value used to indicate this is 'locate'.

        """
        self.cb.orderStatus(OrderStatusMsg(orderId , status, filled,
                                            remaining, avgFillPrice, permId,
                                            parentId, lastFillPrice, clientId,
                                            whyHeld, mktCapPrice))
    
    @logibmsg
    def commissionReport(self, commissionReport):
        """The commissionReport() callback is triggered as follows:
        - immediately after a trade execution
        - by calling reqExecutions()."""
        self.cb.commissionReport(commissionReport)

    @logibmsg
    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson = ""):
        self.cb.error(ErrorMsg(reqId, errorCode, errorString, advancedOrderRejectJson))

    @logibmsg
    def position(self, account, contract, pos, avgCost):
        """This event returns real-time positions for all accounts in
        response to the reqPositions() method."""
        self.cb.position(account, contract, pos, avgCost)

    @logibmsg
    def positionEnd(self):
        """This is called once all position data for a given request are
        received and functions as an end marker for the position() data. """
        self.cb.positionEnd()

    @logibmsg
    def tickPrice(self, reqId, tickType, price, attrib):
        """Market data tick price callback. Handles all price related ticks."""
        self.cb.tickPrice(reqId, tickType, price, attrib)

    @logibmsg
    def tickSize(self, reqId, tickType, size):
        """Market data tick size callback. Handles all size-related ticks."""
        self.cb.tickSize(reqId, tickType, size)

    @logibmsg
    def tickGeneric(self, reqId, tickType, value):
        self.cb.tickGeneric(reqId, tickType, value)

    @logibmsg
    def realtimeBar(self, reqId, time, open_, high, low, close, volume, wap, count):
        self.cb.realtimeBar(RTBar(reqId, time, open_, high, low, close, float(volume), wap, count))

    def historicalData(self, reqId, bar):
        self.cb.historicalData(HistBar(reqId, bar))

    @logibmsg
    def historicalSchedule(self, reqId, start_datetime, end_datetime, timezone, sessions):
        '''Not implemented'''
        pass

    @logibmsg
    def historicalDataUpdate(self, reqId, bar):
        '''Not implemented'''
        pass

    def historicalDataEnd(self, reqId, start, end):
        """ Marks the ending of the historical bars reception. """
        self.cb.historicalDataEnd(reqId, start, end)

    @logibmsg
    def execDetails(self, reqId, contract, execution):
        """This event is fired when the reqExecutions() functions is
        invoked, or when an order is filled.  """
        self.cb.execDetails(reqId, contract, execution)

    @logibmsg
    def execDetailsEnd(self, reqId):
        """This function is called once all executions have been sent to
        a client in response to reqExecutions()."""
        pass

    @logibmsg
    def historicalTicks(self, reqId, ticks, done):
        """For whatToShow=MIDPOINT
        """
        for tick in ticks:
            self.cb.historicalTicks(reqId, HistTick(tick, 'RT_TICK_MIDPOINT'))
        if done:
            self.cb.historicalTicksEnd(reqId)

    @logibmsg
    def historicalTicksBidAsk(self, reqId, ticks, done):
        """returns historical tick data when whatToShow=BID_ASK"""
        for tick in ticks:
            self.cb.historicalTicks(reqId, HistTick(tick, 'RT_TICK_BID_ASK'))
        if done:
            self.cb.historicalTicksEnd(reqId)

    @logibmsg
    def historicalTicksLast(self, reqId, ticks, done):
        """returns tick-by-tick data for tickType = "Last" or "AllLast" """
        for tick in ticks:
            self.cb.historicalTicks(reqId, HistTick(tick, 'RT_TICK_LAST'))
        if done:
            self.cb.historicalTicksEnd(reqId)

    @logibmsg
    def tickByTickAllLast(self, reqId, tickType, time, price, size, tickAtrribLast, exchange, specialConditions):
        """returns tick-by-tick data for tickType = "Last" or "AllLast" """
        self.cb.tickByTickAllLast(reqId, tickType, time, price, size, tickAtrribLast, exchange, specialConditions)

    @logibmsg
    def tickByTickBidAsk(self, reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
        """returns tick-by-tick data for tickType = "BidAsk" """
        self.cb.tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk)

    @logibmsg
    def tickByTickMidPoint(self, reqId, time, midPoint):
        """returns tick-by-tick data for tickType = "MidPoint" """
        self.cb.tickByTickBidAsk(reqId, time, midPoint)

    @logibmsg
    def tickString(self, reqId, tickType, value):
        self.cb.tickString(reqId, tickType, value)

    def run(self):
        store_logger.info("Starting IB API run loop")
        super().run()
        store_logger.info("Exiting IB API run loop")


class IBStore(with_metaclass(MetaSingleton, object)):
    '''Singleton class wrapping an ibpy ibConnection instance.

    The parameters can also be specified in the classes which use this store,
    like ``IBData`` and ``IBBroker``

    Params:

      - ``host`` (default:``127.0.0.1``): where IB TWS or IB Gateway are
        actually running. And although this will usually be the localhost, it
        must not be

      - ``port`` (default: ``7496``): port to connect to. The demo system uses
        ``7497``

      - ``clientId`` (default: ``None``): which clientId to use to connect to
        TWS.

        ``None``: generates a random id between 1 and 65535
        An ``integer``: will be passed as the value to use.

      - ``notifyall`` (default: ``False``)

        If ``False`` only ``error`` messages will be sent to the
        ``notify_store`` methods of ``Cerebro`` and ``Strategy``.

        If ``True``, each and every message received from TWS will be notified

      - ``_debug`` (default: ``False``)

        Print all messages received from TWS as info output

      - ``reconnect`` (default: ``3``)

        Number of attempts to try to reconnect after the 1st connection attempt
        fails

        Set it to a ``-1`` value to keep on reconnecting forever

      - ``timeout`` (default: ``3.0``)

        Time in seconds between reconnection attemps

      - ``timeoffset`` (default: ``True``)

        If True, the time obtained from ``reqCurrentTime`` (IB Server time)
        will be used to calculate the offset to localtime and this offset will
        be used for the price notifications (tickPrice events, for example for
        CASH markets) to modify the locally calculated timestamp.

        The time offset will propagate to other parts of the ``backtrader``
        ecosystem like the **resampling** to align resampling timestamps using
        the calculated offset.

      - ``timerefresh`` (default: ``60.0``)

        Time in seconds: how often the time offset has to be refreshed

      - ``indcash`` (default: ``True``)

        Manage IND codes as if they were cash for price retrieval
    '''

    # Set a base for the data requests (historical/realtime) to distinguish the
    # id in the error notifications from orders, where the basis (usually
    # starting at 1) is set by TWS
    REQIDBASE = 0x01000000

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    BAR_SIZE = {
        '1 secs': {
            'max_duration': 1800,
            'max_duration_name': 'S',
        },
        '5 secs': {
            'max_duration': 3600,
            'max_duration_name': 'S',
        },
        '10 secs': {
            'max_duration': 10800,
            'max_duration_name': 'S',
        },
        '15 secs': {
            'max_duration': 14400,
            'max_duration_name': 'S',
        },
        '30 secs': {
            'max_duration': 28800,
            'max_duration_name': 'S',
        },
        '1 min': {
            'max_duration': 1,
            'max_duration_name': 'D',
        },
        '2 mins': {
            'max_duration': 2,
            'max_duration_name': 'D',
        },
        '3 mins': {
            'max_duration': 1,
            'max_duration_name': 'W',
        },
        '5 mins': {
            'max_duration': 1,
            'max_duration_name': 'W',
        },
        '10 mins': {
            'max_duration': 1,
            'max_duration_name': 'W',
        },
        '15 mins': {
            'max_duration': 2,
            'max_duration_name': 'W',
        },
        '20 mins': {
            'max_duration': 2,
            'max_duration_name': 'W',
        },
        '30 mins': {
            'max_duration': 1,
            'max_duration_name': 'M',
        },
        '1 hour': {
            'max_duration': 1,
            'max_duration_name': 'M',
        },
        '2 hours': {
            'max_duration': 1,
            'max_duration_name': 'M',
        },
        '3 hours': {
            'max_duration': 1,
            'max_duration_name': 'M',
        },
        '4 hours': {
            'max_duration': 1,
            'max_duration_name': 'M',
        },
        '8 hours': {
            'max_duration': 1,
            'max_duration_name': 'M',
        },
        '1 day': {
            'max_duration': 1,
            'max_duration_name': 'Y',
        },
        '1 week': {
            'max_duration': 1,
            'max_duration_name': 'Y',
        },
        '1 month': {
            'max_duration': 1,
            'max_duration_name': 'Y',
        },
        '1 year': {
            'max_duration': 5,
            'max_duration_name': 'Y',
        },
    }

    MAX_DURATION = {
        'S': 86400,
        'D': 365,
    }

    TF2BARSIZE = collections.OrderedDict({
        TimeFrame.Seconds: (
            (1, '1 S', '1 secs'),
            (5, '5 S', '5 secs'),
            (10, '10 S', '10 secs'),
            (15, '15 S', '15 secs'),
            (30, '30 S', '30 secs'),
        ),
        TimeFrame.Minutes: (
            (1, '60 S', '1 min'),
            (2, '120 S', '2 mins'),
            (3, '90 S', '3 mins'),
            (5, '300 S', '5 mins'),
            (10, '600 S', '10 mins'),
            (15, '900 S', '15 mins'),
            (20, '1200 S', '20 mins'),
            (30, '1800 S', '30 mins'),
            (60, '3600 S', '1 hour'),
            (120, '7200 S', '2 hours'),
            (180, '10800 S', '3 hours'),
            (240, '14400 S', '4 hours'),
            (480, '28800 S', '8 hours'),
        ),
        TimeFrame.Days: (
            (1, '1 D', '1 day'),
        ),
        TimeFrame.Weeks: (
            (1, '1 W', '1 week'),
        ),
        TimeFrame.Months: (
            (1, '1 M', '1 month'),
        ),
        TimeFrame.Years: (
            (1, '1 Y', '1 year'),
        ),
    })

    params = (
        ('host', '127.0.0.1'),
        ('port', 7496),
        ('clientId', 8531),  # None generates a random clientid 1 -> 2^16
        ('notifyall', False),
        ('_debug', False),
        ('reconnect', 3),  # -1 forever, 0 No, > 0 number of retries
        ('timeout', 3.0),  # timeout between reconnections
        ('timeoffset', True),  # Use offset to server for timestamps if needed
        ('timerefresh', 60.0),  # How often to refresh the timeoffset
        ('indcash', True),  # Treat IND codes as CASH elements
        ('broker', None),  # broker instance
    )

    @classmethod
    def getdata(cls, *args, **kwargs):
        '''Returns ``DataCls`` with args, kwargs'''
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        '''Returns broker with *args, **kwargs from registered ``BrokerCls``'''
        return cls.BrokerCls(*args, **kwargs)
    
    def __init__(self):
        super(IBStore, self).__init__()

        self._lock_q = threading.Lock()  # sync access to _tickerId/Queues
        self._lock_accupd = threading.Lock()  # sync account updates
        self._lock_pos = threading.Lock()  # sync position updates
        self._lock_notif = threading.Lock()  # sync access to notif queue
        self._lock_managed_acc = threading.Lock()  # sync managed accounts
        self._lock_lose_data = threading.Lock()

        self._lose_data = False
        self._need_reconnect = False

        # Account list received
        self._event_managed_accounts = threading.Event()
        self._event_accdownload = threading.Event()

        self.dontreconnect = False  # for non-recoverable connect errors

        self._env = None  # reference to cerebro for general notifications
        self.broker = None if not self.p.broker else self.p.broker # broker instance
        self.datas = list()  # datas that have registered over start
        self.ccount = 0  # requests to start (from cerebro or datas)

        self._lock_tmoffset = threading.Lock()
        self.tmoffset = datetime.timedelta()  # to control time difference with server

        # Structures to hold datas requests
        self.qs = collections.OrderedDict()  # key: tickerId -> queues
        self.ts = collections.OrderedDict()  # key: queue -> tickerId
        self.iscash = dict()  # tickerIds from cash products (for ex: EUR.JPY)

        self._lock_histdata = threading.Lock()  # sync historical data
        self.histexreq = dict()  # holds segmented historical requests
        self.histfmt = dict()  # holds datetimeformat for request
        self.histsend = dict()  # holds sessionend (data time) for request
        self.histtz = dict()  # holds sessionend (data time) for request

        self.realtz = dict()  # holds realtime bar timezone info

        self.acc_cash = AutoDict()  # current total cash per account
        self.acc_value = AutoDict()  # current total value per account
        self.acc_upds = AutoDict()  # current account valueinfos per account

        self.port_update = False  # indicate whether to signal to broker

        self.positions = collections.defaultdict(Position)  # actual positions

        self._tickerId = itertools.count(self.REQIDBASE)  # unique tickerIds
        self.orderid = None  # next possible orderid (will be itertools.count)

        self.cdetails = collections.defaultdict(list)  # hold cdetails requests

        self.managed_accounts = list()  # received via managedAccounts

        self.notifs = queue.Queue()  # store notifications for cerebro

        # Use the provided clientId or a random one
        if self.p.clientId is None:
            self.clientId = random.randint(1, pow(2, 16) - 1)
        else:
            self.clientId = self.p.clientId

        self._debug = self.p._debug
        self._stop_flag = False

        self.set_logger_level()

        # ibpy connection object
        self.conn = IBApi(self, self._debug)
        self.conn.connect(self.p.host, self.p.port, self.clientId)
        while not self.connected():
            self.conn.connect(self.p.host, self.p.port, self.clientId)
            store_logger.warning("Connect failed retrying after 5 seconds.....")
            time.sleep(5)
        # give time before starting new thread
        # if not done, the new thread starts and immediately receives messages, meanwhile this class IBstore hasn't had time to setup its own depdenencies e.g. self.broker is None
        # cerebro.run will setup store <-> broker deps but it needs time to run
        # todo - place this somewhere after the start method is called - and once data and broker are ready, then this should be invoked, otherwise there will be race conditions, and caller will need to pre-assign the broker to the store first
        self.apiThread = threading.Thread(target=self.conn.run, name="ibapi_run_thread", daemon=True)
        self.apiThread.start()

    def set_logger_level(self):
        handler = store_logger.handlers[0]
        if self._debug:
            handler.setLevel(logging.DEBUG)
            store_logger.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)
            store_logger.setLevel(logging.INFO)

    def start(self, data=None, broker=None):
        store_logger.info(f"IBStore start, data: {data} broker: {broker}")
        while not self.reconnect(fromstart=True):  # reconnect should be an invariant
            store_logger.warning("Connect failed retrying after 5 seconds.....")
            time.sleep(5)

        # Datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        # Datas require some processing to kickstart data reception
        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            # if connection fails, get a fake registration that will force the
            # datas to try to reconnect or else bail out
            return self.getTickerQueue(start=True)

        elif broker is not None:
            self.broker = broker
    
    def stop(self):
        self._stop_flag = True

        try:
            self.conn.disconnect()  # disconnect should be an invariant
        except AttributeError:
            pass    # conn may have never been connected and lack "disconnect"

        # Unblock any calls set on these events
        self._event_managed_accounts.set()
        self._event_accdownload.set()

    def data_is_lost(self):
        result = False
        with self._lock_lose_data:
            result = self._lose_data
        return result

    def set_losing_data(self, value):
        with self._lock_lose_data:
            self._lose_data = value

        if value:
            # when set the data is lost, wait for the reconnect signal
            self._need_reconnect = False

    def need_reconnect(self):
        return self._need_reconnect
    
    # @logibmsg
    def connected(self):
        # The isConnected method is available through __getattr__ indirections
        # and may not be present, which indicates that no connection has been
        # made because the subattribute sender has not yet been created, hence
        # the check for the AttributeError exception
        try:
            return self.conn.isConnected()
        except AttributeError:
            pass

        return False  # non-connected (including non-initialized)

    # @logibmsg
    def reconnect(self, fromstart=False, resub=False):
        # This method must be an invariant in that it can be called several
        # times from the same source and must be consistent. An exampler would
        # be 5 datas which are being received simultaneously and all request a
        # reconnect

        # Policy:
        #  - if dontreconnect has been set, no option to connect is possible
        #  - check connection and use the absence of isConnected as signal of
        #    first ever connection (add 1 to retries too)
        #  - Calculate the retries (forever or not)
        #  - Try to connct
        #  - If achieved and fromstart is false, the datas will be
        #    re-kickstarted to recreate the subscription
        if self._stop_flag:
            return False

        firstconnect = False
        try:
            if self.connected():
                if resub:
                    self.startdatas()
                return True  # nothing to do
        except AttributeError:
            # Not connected, several __getattr__ indirections to
            # self.conn.sender.client.isConnected
            firstconnect = True

        if self.dontreconnect:
            return False

        # This is only invoked from the main thread by datas and therefore no
        # lock is needed to control synchronicity to it
        retries = self.p.reconnect
        if retries >= 0:
            retries += firstconnect

        while retries < 0 or retries:
            store_logger.debug(f"Retries: {retries}")
            if not firstconnect:
                store_logger.debug(f"Reconnect in {self.p.timeout} secs")
                time.sleep(self.p.timeout)

            firstconnect = False

            store_logger.debug("Connect (host={self.p.host}, port={self.p.port}, clientId={self.clientId})")
            self.conn.connect(self.p.host, self.p.port, self.clientId)
            if self.connected():
                if not fromstart or resub:
                    self.startdatas()
                return True  # connection successful

            if retries > 0:
                retries -= 1

        # self.dontreconnect = True
        return False  # connection/reconnection failed

    def startdatas(self):
        # kickstrat datas, not returning until all of them have been done
        ts = list()
        index = 0
        for data in self.datas:
            t = threading.Thread(target=data.reqdata, name="reqdata%d" % index)
            t.start()
            ts.append(t)
            index += 1

        for t in ts:
            t.join()

    @logibmsg
    def stopdatas(self, force=False):
        # if no stop flag, don't stop datas
        # and wait for reconnecting
        if not self._stop_flag and not force:
            return

        # stop subs and force datas out of the loop (in LIFO order)
        store_logger.info(f"Stopping datas")
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.canceldata, name="canceldata")
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

        with self._lock_q:
            qs = list(self.qs.values())
        for q in reversed(qs):  # datamaster the last one to get a None
            q.put(None)

    def get_notifications(self):
        '''Return the pending "store" notifications'''
        # The background thread could keep on adding notifications. The None
        # mark allows to identify which is the last notification to deliver
        self.notifs.put(None)  # put a mark
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:  # mark is reached
                break
            notifs.append(notif)

        return notifs
    
    def error(self, msg):
        # 100-199 Order/Data/Historical related
        # 200-203 tickerId and Order Related
        # 300-399 A mix of things: orders, connectivity, tickers, misc errors
        # 400-449 Seem order related again
        # 500-531 Connectivity/Communication Errors
        # 10000-100027 Mix of special orders/routing
        # 1100-1102 TWS connectivy to the outside
        # 1300- Socket dropped in client-TWS communication
        # 2100-2110 Informative about Data Farm status (id=-1)

        # All errors are logged to the environment (cerebro), because many
        # errors in Interactive Brokers are actually informational and many may
        # actually be of interest to the user
        if msg.reqId > 0:
            store_logger.error(f"{msg}")
        elif msg.reqId != -1:
            store_logger.info(f"{msg}")

        if msg.reqId == -1:
            if msg.errorCode == 502:
                store_logger.warning(msg.errorString)
            elif msg.errorCode in [2104, 2107, 2106, 2158]:
                store_logger.info(f"{msg.errorString}")
            elif msg.errorCode == 2105:
                store_logger.critical(f"{msg.errorString}")
                # raise RuntimeError("We cannot get the historical data from interactive brokers, please restart the ibgate or tws!")
            else:
                store_logger.error(f"error: {msg.errorCode} {msg.errorString}")

        if not self.p.notifyall:
            self.notifs.put((msg, tuple(vars(msg).values()), dict(vars(msg).items())))

        # Manage those events which have to do with connection
        if msg.errorCode is None:
            # Usually received as an error in connection of just before disconn
            pass

        elif msg.errorCode in [162, 320, 321, 322]:
            # try to fetch the data again and again
            try:
                with self._lock_q:
                    q = self.qs[msg.reqId]
            except KeyError:
                store_logger.warn(f"Cancel data queue for {msg.reqId} failed. Cannot find the queue")
            else:
                store_logger.info(f"Put error code and cancel data queue for {msg.reqId} to avoid memory leak")
                q.put(msg.errorCode)
                self.cancelQueue(q)

        elif msg.errorCode == 165:  #  Historical Market Data Service query message:HMDS server connection was successful.
            # do nothing
            pass

        elif msg.errorCode in [200, 203]:
            # cdetails 200 security not found, notify over right queue
            # cdetails 203 security not allowed for acct
            try:
                with self._lock_q:
                    q = self.qs[msg.reqId]
            except KeyError:
                pass  # should not happend but it can
            else:
                store_logger.warn(f"Cancel data queue for {msg.reqId}")
                self.cancelQueue(q, True)

        elif msg.errorCode in [354, 420]:
            # 354 no subscription, 420 no real-time bar for contract
            # the calling data to let the data know ... it cannot resub
            try:
                with self._lock_q:
                    q = self.qs[msg.reqId]
            except KeyError:
                pass  # should not happend but it can
            else:
                q.put(-msg.errorCode)
                store_logger.warn(f"Cancel data queue for {msg.reqId}")
                self.cancelQueue(q)

        elif msg.errorCode == 326:  # not recoverable, clientId in use
            self.dontreconnect = True
            store_logger.error("IBStore: clientId in use, cannot reconnect, eixt!!!!")
            self.stop()
            self.close_connection()

        elif msg.errorCode in [502, 504, 10225, 1101, 1102]:
            # Cannot connect to TWS: port, config not open, tws off (504 then)
            # 1101 https://interactivebrokers.github.io/tws-api/message_codes.html responds with message like
            # ibapi_wrapper.ibstore - ERROR - error: 1102 Connectivity between IBKR and Trader Workstation has been restored - data maintained. All data farms are connected: usfarm.nj; usfuture; cashfarm; usopt; usfarm; euhmds; fundfarm; ushmds; secdefnj.
            for data in self.datas:
                if data is not None:
                    data.push_error(msg)
            self.set_losing_data(True)
            self._need_reconnect = True

        elif msg.errorCode == 1300:
            # TWS has been closed. The port for a new connection is there
            # newport = int(msg.errorMsg.split('-')[-1])  # bla bla bla -7496
            self.dontreconnect = True
            store_logger.error("IBStore: TWS has been closed, cannot reconnect, exit!!!!, receive 1300")
            self.stop()
            self.close_connection()

        elif msg.errorCode == 1100:
            # Connection restored and tickerIds are gone
            for data in self.datas:
                if data is not None:
                    data.push_error(msg)

        elif msg.errorCode < 500:
            # Given the myriad of errorCodes, start by assuming is an order
            # error and if not, the checks there will let it go
            if msg.reqId < self.REQIDBASE:
                if self.broker is not None:
                    self.broker.push_ordererror(msg)
            else:
                # Cancel the queue if a "data" reqId error is given: sanity
                with self._lock_q:
                    q = self.qs[msg.reqId]
                store_logger.warn(f"Cancel data queue for {msg.reqId}")
                self.cancelQueue(q, True)
    
    def connectionClosed(self):
        # Sometmes this comes without 1300/502 or any other and will not be
        # seen in error hence the need to manage the situation independently
        self.close_connection()

    def close_connection(self):
        if not self._stop_flag:
            return False

        if self.connected():
            self.stopdatas()
            self.conn.disconnect()
        return True
    
    def force_close_connection(self):
        if self.connected():
            self.stopdatas(force=True)
            self.conn.disconnect()
            return True
        else:
            return False
    
    def updateAccountTime(self, timeStamp):
        store_logger.debug(f"timeStamp: {timeStamp}")

    def connectAck(self):
        store_logger.debug(f"connectAck")
    
    def managedAccounts(self, accountsList):
        # 1st message in the stream
        with self._lock_managed_acc:
            self.managed_accounts = accountsList.split(',')
        self._event_managed_accounts.set()

        # Request time to avoid synchronization issues
        self.reqCurrentTime()

    @logibmsg
    def reqCurrentTime(self):
        self.conn.reqCurrentTime()
    
    def currentTime(self, time):
        if not self.p.timeoffset:  # only if requested ... apply timeoffset
            return
        curtime = datetime.datetime.fromtimestamp(float(time))
        with self._lock_tmoffset:
            self.tmoffset = curtime - datetime.datetime.now()
            store_logger.info(f"Server time offset is {self.tmoffset.total_seconds()} seconds")
    
    def timeoffset(self):
        with self._lock_tmoffset:
            return self.tmoffset

    def nextTickerId(self):
        # Get the next ticker using next on the itertools.count
        return next(self._tickerId)

    def nextValidId(self, orderId):
        # Create a counter from the TWS notified value to apply to orders
        self.orderid = itertools.count(orderId)

    def nextOrderId(self):
        # Get the next ticker using next on the itertools.count made with the
        # notified value from TWS
        return next(self.orderid)

    def reuseQueue(self, tickerId):
        '''Reuses queue for tickerId, returning the new tickerId and q'''
        with self._lock_q:
            # Invalidate tickerId in qs (where it is a key)
            q = self.qs.pop(tickerId, None)  # invalidate old
            iscash = self.iscash.pop(tickerId, None)

            # Update ts: q -> ticker
            tickerId = self.nextTickerId()  # get new tickerId
            self.ts[q] = tickerId  # Update ts: q -> tickerId
            self.qs[tickerId] = q  # Update qs: tickerId -> q
            self.iscash[tickerId] = iscash

        return tickerId, q
    
    def getTickerQueue(self, start=False):
        '''Creates ticker/Queue for data delivery to a data feed'''
        q = queue.Queue()
        if start:
            q.put(None)
            return q

        with self._lock_q:
            tickerId = self.nextTickerId()
            self.qs[tickerId] = q  # can be managed from other thread
            self.ts[q] = tickerId
            self.iscash[tickerId] = False

        return tickerId, q
    
    def cancelQueue(self, q, sendnone=False):
        '''Cancels a Queue for data delivery'''
        # pop ts (tickers) and with the result qs (queues)
        with self._lock_q:
            tickerId = self.ts.pop(q, None)
            self.qs.pop(tickerId, None)
            self.iscash.pop(tickerId, None)

        if sendnone:
            q.put(None)
    
    def validQueue(self, q):
        '''Returns (bool)  if a queue is still valid'''
        with self._lock_q:
            return q in self.ts  # queue -> ticker
    
    def getContractDetails(self, contract, maxcount=None):
        cds = list()
        q = self.reqContractDetails(contract)
        while True:
            msg = q.get()
            if msg is None:
                break
            cds.append(msg)

        if not cds or (maxcount and len(cds) > maxcount):
            err = 'Ambiguous contract: none/multiple answers received'
            self.notifs.put((err, cds, {}))
            return None

        return cds
    
    def reqContractDetails(self, contract):
        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()
        self.conn.reqContractDetails(tickerId, contract)
        return q

    def contractDetailsEnd(self, reqId):
        '''Signal end of contractdetails'''
        store_logger.debug(f"Cancel data queue tickerId: {reqId} Q: {self.qs[reqId]}")
        with self._lock_q:
            tickerId = self.qs[reqId]
        self.cancelQueue(tickerId, True)
        
    def contractDetails(self, reqId, contractDetails):
        '''Receive answer and pass it to the queue'''
        with self._lock_q:
            q = self.qs[reqId]
        q.put(contractDetails)

    def reqHistoricalDataEx(self, contract, enddate, begindate,
                            timeframe, compression,
                            what=None, useRTH=False, tz='', sessionend=None,
                            tickerId=None, useSplit=False):
        '''
        Extension of the raw reqHistoricalData proxy, which takes two dates
        rather than a duration, barsize and date

        It uses the IB published valid duration/barsizes to make a mapping and
        spread a historical request over several historical requests if needed
        '''
        # Keep a copy for error reporting purposes
        kwargs = locals().copy()
        kwargs.pop('self', None)  # remove self, no need to report it

        if timeframe < TimeFrame.Seconds:
            # Ticks are not supported
            return self.getTickerQueue(start=True)

        if enddate is None:
            raise ValueError('enddate must be specified when request historical Data')

        # Check if the requested timeframe/compression is supported by IB
        data_compression, base_duration, barsize = self.get_barsize(timeframe, compression)
        if not data_compression:  # return a queue and put a None in it
            raise ValueError(f'Invalid timeframe/compression {timeframe}/{compression}')

        # Get or reuse a queue
        if tickerId is None:
            tickerId, q = self.getTickerQueue()
            store_logger.debug(f"Get tickerId: {tickerId} Q: {q}")
        else:
            tickerId, q = self.reuseQueue(tickerId)  # reuse q for old tickerId
            store_logger.debug(f"Reuse tickerId: {tickerId} Q: {q}")

        store_logger.info(f"Request duration: {base_duration} barsize: {barsize} timeframe: {timeframe} compression: {compression}")
        store_logger.info(f"Date: {enddate} begindate: {begindate} what: {what} useRTH: {useRTH} tz: {tz} sessionend: {sessionend}")
        if begindate is None:
            duration = self.get_max_duration(barsize)
        else:
            if useSplit:
                begindate, splitend, duration, finished = self.dt_split_duration(begindate, enddate, base_duration)
                if not finished:
                    with self._lock_histdata:
                        self.histexreq[tickerId] = {
                            'contract': contract,
                            'enddate': enddate,
                            'begindate': splitend,
                            'timeframe': timeframe,
                            'compression': compression,
                            'what': what,
                            'useRTH': useRTH,
                            'tz': tz,
                            'sessionend': sessionend,
                            'useSplit': useSplit,
                        }
                enddate = splitend
            else:
                # Get the best possible duration to reduce number of requests
                begindate, duration = self.dt_plus_duration(begindate, enddate, base_duration)

        duration = self.check_max_duration(barsize, duration)

        if duration is None:  # no duration large enough to fit the request
            raise RuntimeError(f"Cannot find historical duration for {begindate} to {enddate} on {base_duration}")

        with self._lock_histdata:
            self.histfmt[tickerId] = timeframe >= TimeFrame.Seconds
            self.histsend[tickerId] = sessionend
            self.histtz[tickerId] = tz

        if contract.secType in ['CASH', 'CFD']:
            with self._lock_q:
                self.iscash[tickerId] = 1  # msg.field code
            if not what:
                what = 'BID'  # default for cash unless otherwise specified

        elif contract.secType in ['IND'] and self.p.indcash:
            with self._lock_q:
                self.iscash[tickerId] = 4  # msg.field code

        what = what or 'TRADES'

        store_logger.info(f"Request Historical Data, parameters are:")
        store_logger.info(f"tickerId: {tickerId} contract: {contract} enddate: {enddate} begindate: {begindate} duration: {duration} barsize: {barsize} what: {what}, useRTH: {useRTH} tz: {tz}")
        self.conn.reqHistoricalData(
            tickerId,
            contract,
            bytes(enddate.strftime('%Y%m%d-%H:%M:%S')),
            bytes(duration),
            bytes(barsize),
            bytes(what),
            int(useRTH),
            1, # dateformat 1 for string, 2 for unix time in seconds
            False,
            [])

        return q
    
    def reqHistoricalTicksEx(self, contract, enddate=None, begindate=None,
                            what=None, useRTH=False, tz='',
                            tickerId=None):
        '''
        Extension of the raw reqHistoricalData proxy, which takes two dates
        rather than a duration, barsize and date

        It uses the IB published valid duration/barsizes to make a mapping and
        spread a historical request over several historical requests if needed
        '''
        # Keep a copy for error reporting purposes
        kwargs = locals().copy()
        kwargs.pop('self', None)  # remove self, no need to report it

        if enddate and begindate:
            err = ('Only fromdate OR enddate can be specified not both')
            self.notifs.put((err, (), kwargs))
            return self.getTickerQueue(start=True)

        if enddate is None and begindate is None:
            today = datetime.datetime.utcnow().date()
            begindate = datetime.datetime(today.year, today.month, today.day)
            # begindate = datetime.datetime.now()

        store_logger.debug(f"begin: {begindate} end: {enddate}")

        # Get or reuse a queue
        if tickerId is None:
            tickerId, q = self.getTickerQueue()
            store_logger.debug(f"Get tickerId: {tickerId} Q: {q}")
        else:
            tickerId, q = self.reuseQueue(tickerId)  # reuse q for old tickerId
            store_logger.debug(f"Reuse tickerId: {tickerId} Q: {q}")

        if contract.secType in ['CASH', 'CFD']:
            with self._lock_q:
                self.iscash[tickerId] = 1  # msg.field code
            if not what:
                what = 'BID'  # default for cash unless otherwise specified

        elif contract.secType in ['IND'] and self.p.indcash:
            with self._lock_q:
                self.iscash[tickerId] = 4  # msg.field code

        what = what or 'TRADES'

        self.conn.reqHistoricalTicks(
            tickerId,
            contract,
            # bytes(begindate.strftime('%Y%m%d %H:%M:%S') + ' GMT') if begindate else '',
            # bytes(enddate.strftime('%Y%m%d %H:%M:%S') + ' GMT') if enddate else '',
            bytes(begindate.strftime('%Y%m%d-%H:%M:%S')) if begindate else '',
            bytes(enddate.strftime('%Y%m%d-%H:%M:%S')) if enddate else '',
            999,
            bytes(what),
            int(useRTH),
            True,
            [])

        return q

    def cancelHistoricalData(self, q):
        '''Cancels an existing HistoricalData request

        Params:
          - q: the Queue returned by reqMktData
        '''
        with self._lock_q:
            tickerId = self.ts[q]
        self.conn.cancelHistoricalData(tickerId)
        store_logger.warn(f"Cancel data queue for {q}")
        self.cancelQueue(q, True)

    @logibmsg
    def reqRealTimeBars(self, contract, useRTH=False, duration=5, what=None, tz=None):
        '''Creates a request for (5 seconds) Real Time Bars

        Params:
          - contract: a ib.ext.Contract.Contract intance
          - useRTH: (default: False) passed to TWS
          - duration: (default: 5) passed to TWS, no other value works in 2016)

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        '''
        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()

        what = what or 'TRADES'

        # 20150929 - Only 5 secs supported for duration
        self.conn.reqRealTimeBars(
            tickerId,
            contract,
            duration,
            # bytes('TRADES'),
            bytes(what),
            useRTH,
            [])
        self.realtz[tickerId] = tz

        store_logger.info(f"Request realtime data queue for {tickerId}")
        return q

    def cancelRealTimeBars(self, q):
        '''Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqMktData
        '''
        with self._lock_q:
            tickerId = self.ts.get(q, None)
        if tickerId is not None:
            self.conn.cancelRealTimeBars(tickerId)

        store_logger.info(f"Cancel data queue for {tickerId}")
        self.cancelQueue(q, True)

    def reqMktData(self, contract, what=None):
        '''Creates a MarketData subscription

        Params:
          - contract: a ib.ext.Contract.Contract intance

        Returns:
          - a Queue the client can wait on to receive a RTVolume instance
        '''
        # get a ticker/queue for identification/data delivery
        tickerId, q = self.getTickerQueue()
        # Low 13 Weeks	Lowest price for the last 13 weeks. For stocks only.	165	IBApi.EWrapper.tickPrice	15
        # High 13 Weeks	Highest price for the last 13 weeks. For stocks only.	165	IBApi.EWrapper.tickPrice	16
        # Low 26 Weeks	Lowest price for the last 26 weeks. For stocks only.	165	IBApi.EWrapper.tickPrice	17
        # High 26 Weeks	Highest price for the last 26 weeks. For stocks only.	165	IBApi.EWrapper.tickPrice	18
        # Low 52 Weeks	Lowest price for the last 52 weeks. For stocks only.	165	IBApi.EWrapper.tickPrice	19
        # High 52 Weeks	Highest price for the last 52 weeks. For stocks only.	165	IBApi.EWrapper.tickPrice	20
        # Average Volume	The average daily trading volume over 90 days. Multiplier of 100. For stocks only.	165	IBApi.EWrapper.tickSize	21
        # Mark Price	“The mark price is the current theoretical calculated value of an instrument. Since it is a calculated value it will typically have many digits of precision.”	232	IBApi.EWrapper.tickPrice	37
        # Shortable	Describes the level of difficulty with which the contract can be sold short. See Shortable	236	IBApi.EWrapper.tickGeneric	46
        # Trade Count	Trade count for the day.	293	IBApi.EWrapper.tickGeneric	54
        # Trade Rate	Trade count per minute.	294	IBApi.EWrapper.tickGeneric	55
        # Volume Rate	Volume per minute.	295	IBApi.EWrapper.tickGeneric	56
        # Last RTH Trade	Last Regular Trading Hours traded price.	318	IBApi.EWrapper.tickPrice	57
        # RT Historical Volatility	30-day real time historical volatility.	411	IBApi.EWrapper.tickGeneric	58
        # Short-Term Volume 3 Minutes	The past three minutes volume. Interpolation may be applied. For stocks only.	595	IBApi.EWrapper.tickSize	63
        # Short-Term Volume 5 Minutes	The past five minutes volume. Interpolation may be applied. For stocks only.	595	IBApi.EWrapper.tickSize	64
        # Short-Term Volume 10 Minutes	The past ten minutes volume. Interpolation may be applied. For stocks only.	595	IBApi.EWrapper.tickSize	65

        ticks = '165,232,233,236,293,294,295,318,411,595'  # request RTVOLUME tick delivered over tickString

        if contract.secType in ['CASH', 'CFD']:
            with self._lock_q:
                self.iscash[tickerId] = True
                ticks = ''  # cash markets do not get RTVOLUME
                if what == 'ASK':
                    self.iscash[tickerId] = 2

        # request live data
        store_logger.info(f"Request Market Data, parameters are: {tickerId} {contract}, {ticks}")
        self.conn.reqMarketDataType(1)
        self.conn.reqMktData(tickerId, contract, bytes(ticks), False, False, [])
        return q

    def reqTickByTickData(self, contract, what=None, ignoreSize=True):
        '''
        Tick-by-tick data corresponding to the data shown in the 
        TWS Time & Sales Window is available starting with TWS v969 and API v973.04.
        '''    

        if what == 'TRADES':
            what = 'Last'
        elif what == 'TRADES_ALL':
            what = 'AllLast'
        elif what == 'BID_ASK':
            what = 'BidAsk'
        elif what == 'MIDPOINT':
            what = 'MidPoint'
        else:
            what = 'Last'

        tickerId, q = self.getTickerQueue()
        self.conn.reqTickByTickData(tickerId, contract, what, 0, ignoreSize)
        return q
    
    def cancelMktData(self, q):
        '''Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqMktData
        '''
        with self._lock_q:
            tickerId = self.ts.get(q, None)
        if tickerId is not None:
            self.conn.cancelMktData(tickerId)

        store_logger.debug(f"Cancel data queue for {tickerId}")
        self.cancelQueue(q, True)

    def cancelTickByTickData(self, q):
        '''Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqTickByTickData
        '''
        with self._lock_q:
            tickerId = self.ts.get(q, None)
        if tickerId is not None:
            self.conn.cancelTickByTickData(tickerId)

        store_logger.debug(f"Cancel data queue for {tickerId}")
        self.cancelQueue(q, True)
    
    def tickString(self, reqId, field: int, value: float):
        """Receive and process a tickString message"""
        tickId = reqId
        rtdata = RTTickData(field, value)
        if rtdata.is_valid():
            with self._lock_q:
                q = self.qs[tickId]
            q.put(rtdata)

    def tickPrice(self, reqId, field: int, price: float, attrib: TickAttrib):
        "Receives a tickPrice message"
        # The meaning of the integer value is mentioned in the IB API: https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/#available-tick-types
        tickerId = reqId
        rtdata = RTTickData(field, price, attrib)
        if rtdata.is_valid():
            with self._lock_q:
                q = self.qs[tickerId]
            q.put(rtdata)

    def tickSize(self, reqId, field:int, value: float):
        "Receives a tickSize message"
        tickerId = reqId
        rtsize = RTTickData(field, value)
        if rtsize.is_valid():
            with self._lock_q:
                q = self.qs[tickerId]
            q.put(rtsize)

    def tickGeneric(self, reqId, field: int, value: float):
        '''Receives a tickGeneric message'''
        tickerId = reqId
        if field == 54:  # Trade Count, Trade count for the day.
            store_logger.info(f"today's trade count: {value}")
        elif field == 49:  # contract halt
            if value == -1:  # Halted status not available. Usually returned with frozen data.
                store_logger.info("contract halt: Halted status not available. Usually returned with frozen data.")
            elif value == 0:  # Not halted. This value will only be returned if the contract is in a TWS watchlist.
                store_logger.info("contract halt: Not halted.")
            elif value == 1:  # General halt. Trading halt is imposed for purely regulatory reasons with/without volatility halt.
                store_logger.warn("contract halt: General halt. !!!!!!!!!!")
            elif value == 2:  # Volatility halt. Trading halt is imposed due to volatility and not regulatory reasons.
                store_logger.error("contract halt: Volatility halt.!!!!!!!!!!!!!!")
            else:
                store_logger.warn(f"contract halt: {value} is not a valid value.")
        elif field == 55:  # Trade Rate, Trade count per minute.
            store_logger.info(f"trade rate per minute: {value}")
        elif field == 56:  # Volume Rate, Volume per minute.
            store_logger.info(f"volume rate per minute: {value}")
        elif field == 58:  # RT Historical Volatility, 30-day real time historical volatility.
            store_logger.info(f"30-day real time historical volatility: {value}")
        elif field == 46:  # Describes the level of difficulty with which the contract can be sold short. See Shortable
            if value > 2.5:
                store_logger.info("Shortable: Easy to borrow")
            elif value > 1.5:
                store_logger.info("Shortable: Available")
            else:
                store_logger.info("Shortable: Hard to borrow")

    def realtimeBar(self, msg):
        '''Receives x seconds Real Time Bars (at the time of writing only 5
        seconds are supported)

        Not valid for cash markets
        '''
        # Get a naive localtime object
        tz = self.realtz[msg.reqId]
        local_time = datetime.datetime.fromtimestamp(float(msg.time))
        local_time.replace(tzinfo=TZLocal)
        msg.time = local_time.astimezone(tz)
        with self._lock_q:
            q = self.qs[msg.reqId]
        q.put(msg)

    def historicalData(self, msg):
        '''Receives the events of a historical data request'''
        # For multi-tiered downloads we'd need to rebind the queue to a new
        # tickerId (in case tickerIds are not reusable) and instead of putting
        # None, issue a new reqHistData with the new data and move formward
        tickerId = msg.reqId
        with self._lock_q:
            q = self.qs[tickerId]

        dtstr = msg.date  # Format when string req: YYYYMMDD[  HH:MM:SS] or 20240510 09:29:30 US/Eastern
        has_seconds = False
        has_timezone = False
        timezone = None
        split_dtstr = dtstr.split(" ")
        if len(split_dtstr) == 3:
            dtstr_list = dtstr.rsplit(" ", 1)
            dtstr = dtstr_list[0]
            timezone = dtstr_list[1]
            has_seconds = True
            has_timezone = True
        elif len(split_dtstr) == 2:
            has_seconds = True

        with self._lock_histdata:
            has_histfmt = self.histfmt[tickerId]
        if has_histfmt:
            with self._lock_histdata:
                sessionend = self.histsend[tickerId]
            if has_seconds:
                dt = datetime.datetime.strptime(dtstr, '%Y%m%d %H:%M:%S')
                dteos = dt
            else:
                dt = datetime.datetime.strptime(dtstr, '%Y%m%d')
                dteos = datetime.datetime.combine(dt, sessionend)
            with self._lock_histdata:
                tz = self.histtz[tickerId]
            if tz:
                if has_timezone:
                    date_tz = pytz.timezone(timezone)
                    dteostz = date_tz.localize(dteos)
                else:
                    dteostz = tz.localize(dteos)
                # dteosutc = dteostz.astimezone(pytz.utc)
                # When requesting for example daily bars, the current day
                # will be returned with the already happened data. If the
                # session end were added, the new ticks wouldn't make it
                # through, because they happen before the end of time
            else:
                dteostz = dt

            msg.date = dteostz
        else:
            msg.date = datetime.datetime.utcfromtimestamp(long(dtstr))

        q.put(msg)
    
    def historicalDataEnd(self, reqId, start, end):
        tickerId = reqId
        with self._lock_histdata:
            self.histfmt.pop(tickerId, None)
            self.histsend.pop(tickerId, None)
            self.histtz.pop(tickerId, None)
            kargs = self.histexreq.pop(tickerId, None)
        if kargs is not None:
            q = self.reqHistoricalDataEx(tickerId=tickerId, **kargs)
            q.put("WaitSplit")
            return

        with self._lock_q:
            q = self.qs[tickerId]
        self.cancelQueue(q, True)

    def historicalTicks(self, reqId, tick):
        tickerId = reqId
        with self._lock_q:
            q = self.qs[tickerId]
        q.put(tick)

    def historicalTicksEnd(self, reqId):
        tickerId = reqId
        with self._lock_q:
            q = self.qs[tickerId]
        self.cancelTickByTickData(q)

    def tickByTickBidAsk(self, reqId, time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk):
        tickerId = reqId
        tick = RTTickBidAsk(time, bidPrice, askPrice, bidSize, askSize, tickAttribBidAsk)
        with self._lock_q:
            q = self.qs[tickerId]
        q.put(tick)

    def tickByTickAllLast(self, reqId, tickType, time, price, size, tickAtrribLast, exchange, specialConditions):
        tickerId = reqId
        tick = RTTickLast(tickType, time, price, size, tickAtrribLast, exchange, specialConditions)
        with self._lock_q:
            q = self.qs[tickerId]
        q.put(tick)

    def tickByTickMidPoint(self, reqId, time, midPoint):
        tickerId = reqId
        tick = RTTickMidPoint(time, time, midPoint)
        with self._lock_q:
            q = self.qs[tickerId]
        q.put(tick)

    def get_barsize(self,  timeframe, compression):
        """
        frome timeframe and compression get the possible durations
        """
        # Get TF2BARSIZE[timeframe], find the proper barsize for compression
        bar_size_info = IBStore.TF2BARSIZE[timeframe]
        for comp_info in reversed(bar_size_info):
            # the base of the compression in TF2BARSIZE is the 1 under each key, 
            # so we can find the proper compression anyway
            if comp_info[0] <= compression and compression % comp_info[0] == 0:
                return comp_info[0], comp_info[1], comp_info[2]

        return None, None, None

    def check_max_duration(self, bar_size, duration):
        """
        get the maximum duration for the bar_size
        """
        def get_delta_time(duration):
            duration_list = duration.split()
            size = int(duration_list[0])
            if size <= 0:
                return None
            dim = duration_list[1]
            if dim == "S":
                delta = datetime.timedelta(seconds=size)
            elif dim == "D":
                delta = datetime.timedelta(days=size)
            elif dim == "W":
                delta = datetime.timedelta(weeks=size)
            elif dim == "M":
                delta = datetime.timedelta(days=size * 30)
            elif dim == "Y":
                delta = datetime.timedelta(days=size * 365)
            return delta

        if bar_size not in IBStore.BAR_SIZE:
            raise ValueError(f"Invalid bar size: {bar_size}")
        bar_size_info = IBStore.BAR_SIZE[bar_size]
        delta = get_delta_time(duration)

        max_duration = f"{bar_size_info['max_duration']} {bar_size_info['max_duration_name']}"
        max_duration_delta = get_delta_time(max_duration)

        if delta is None:
            return max_duration
        elif delta > max_duration_delta:
            return max_duration
        else:
            return duration

    def get_max_duration(self, bar_size):
        """
        get the maximum duration for the bar_size
        """
        if bar_size not in IBStore.BAR_SIZE:
            raise ValueError(f"Invalid bar size: {bar_size}")
        bar_size_info = IBStore.BAR_SIZE[bar_size]
        duration = bar_size_info["max_duration"]
        duration_name = bar_size_info["max_duration_name"]
        return f"{duration} {duration_name}"

    def dt_plus_duration(self, dtbegin, dtend, base_duration):
        size, dim = base_duration.split()
        base_size = int(size)

        delta_time = dtend - dtbegin
        # the minimum duration is 1 second
        seconds = int(delta_time.total_seconds())
        if dim == 'S':
            # The step is base_size seconds
            value = math.ceil(seconds / base_size) * base_size
            # ib supports duration in 86400 seconds
            if value > IBStore.MAX_DURATION['S']:
                value = math.ceil(value / IBStore.MAX_DURATION['S'])
                return dtend - datetime.timedelta(days=value), f"{value} D"
            else:
                return dtend - datetime.timedelta(seconds=value), f"{value} S"
        elif dim == 'D':
            value = math.ceil(seconds/ 86400 / base_size) * base_size
            if value > IBStore.MAX_DURATION['D']:
                value = value / IBStore.MAX_DURATION['D']
                return dtend - datetime.timedelta(days=IBStore.MAX_DURATION['D']), f"{value} Y"
            else:
                return dtend - datetime.timedelta(days=value), f"{value} D"
        elif dim == 'W':
            value = math.ceil(seconds / 604800 / base_size) * base_size
            return dtend - datetime.timedelta(days=(value * 7)), f"{value} W"
        elif dim == 'M':
            value = math.ceil(seconds / 2592000 / base_size) * base_size
            return dtend - datetime.timedelta(days=(value * 30)), f"{value} M"
        elif dim == 'Y':
            value = math.ceil(seconds / 31536000 / base_size) * base_size
            return dtend - datetime.timedelta(days=(value * 365)), f"{value} Y"
        else:
            return None, None

    def dt_split_duration(self, dtbegin, dtend, base_duration):
        if base_duration != "1 D":
            raise RuntimeError("split duration only support 1 D")

        # max duration
        delta_days = math.ceil((dtend - dtbegin).total_seconds() / 86400)
        if delta_days > IBStore.MAX_DURATION['D']:
            split_end = dtbegin + datetime.timedelta(days=IBStore.MAX_DURATION['D'])
            return dtbegin, split_end, f"{IBStore.MAX_DURATION['D']} D", False
        else:
            return dtbegin, dtend, f"{delta_days} D", True

    def makecontract(self, symbol, sectype, exch, curr,
                     expiry='', strike=0.0, right='', mult=1, 
                     primaryExch=None, localSymbol=None):
        '''returns a contract from the parameters without check'''

        contract = Contract()
        
        if localSymbol:
            contract.localSymbol = bytes(localSymbol)
        else:
            contract.symbol = bytes(symbol)
        
        contract.secType = bytes(sectype)
        contract.exchange = bytes(exch)
        if primaryExch:
            contract.primaryExchange = bytes(primaryExch)
        if curr:
            contract.currency = bytes(curr)
        if sectype in ['FUT', 'OPT', 'FOP']:
            contract.lastTradeDateOrContractMonth = bytes(expiry)
        if sectype in ['OPT', 'FOP']:
            contract.strike = strike
            contract.right = bytes(right)
        if mult:
            contract.multiplier = bytes(mult)
        return contract

    def cancelOrder(self, orderid, manual_time=""):
        '''Proxy to cancelOrder'''
        self.conn.cancelOrder(orderid, manual_time)
    
    def placeOrder(self, orderid, contract, order):
        '''Proxy to placeOrder'''
        self.conn.placeOrder(orderid, contract, order)

    def reqOpenOrders(self):
        self.conn.reqOpenOrders()
    
    def openOrder(self, msg):
        '''Receive the event ``openOrder`` events'''
        self.broker.push_openorder(msg)

    def openOrderEnd(self):
        '''Receive the event ``openOrderEnd`` events'''
        self.broker.push_openorder()

    def reqCompletedOrders(self):
        self.conn.reqCompletedOrders(True)

    def completedOrders(self, msg):
        self.broker.push_completedorders(msg)
    
    def execDetails(self, reqId, contract, execution):
        '''Receive execDetails'''
        execution.shares = float(execution.shares)
        execution.cumQty = float(execution.cumQty)
        self.broker.push_execution(execution)
    
    def orderStatus(self, msg):
        '''Receive the event ``orderStatus``'''
        store_logger.info(f"Receive order status, msg.orderId: {msg.orderId}, msg.status: {msg.status}")
        self.broker.push_orderstatus(msg)
    
    def commissionReport(self, commissionReport):
        '''Receive the event commissionReport'''
        self.broker.push_commissionreport(commissionReport)

    def reqPositions(self):
        '''Proxy to reqPositions'''
        self.conn.reqPositions()
        
    def position(self, account, contract, pos, avgCost):
        '''Receive event positions'''
        # Lock access to the position dicts. This is called in sub-thread and
        # can kick in at any time
        try:
            if not self._event_accdownload.is_set():  # 1st event seen
                position = Position(float(pos), float(avgCost))
                store_logger.debug(f"POSITIONS INITIAL: {self.positions}")
                with self._lock_pos:
                    self.positions[contract.conId] = position
            else:
                with self._lock_pos:
                    position = self.positions[contract.conId]
                    fix_result = position.fix(float(pos), avgCost)
                store_logger.debug(f"POSITION UPDATE: {position}")
                if not fix_result:
                    err = ('The current calculated position and '
                        'the position reported by the broker do not match. '
                        'Operation can continue, but the trades '
                        'calculated in the strategy may be wrong')

                    self.notifs.put((err, (), {}))

                # self.broker.push_portupdate()
        except Exception as e:
            store_logger.exception(f"Exception: {e}")

    def positionEnd(self):
        store_logger.debug(f"positionEnd")

    def reqAccountUpdates(self, subscribe=True, account=None):
        '''Proxy to reqAccountUpdates

        If ``account`` is ``None``, wait for the ``managedAccounts`` message to
        set the account codes
        '''
        if account is None:
            self._event_managed_accounts.wait()
            with self._lock_managed_acc:
                account = self.managed_accounts[0]

        self.conn.reqAccountUpdates(subscribe, bytes(account))

    def reqManagedAccts(self):
        self.conn.reqManagedAccts()

    def accountDownloadEnd(self, accountName):
        # Signals the end of an account update
        # the event indicates it's over. It's only false once, and can be used
        # to find out if it has at least been downloaded once
        self._event_accdownload.set()
        if False:
            if self.port_update:
                self.broker.push_portupdate()

                self.port_update = False

    def updatePortfolio(self, contract, pos,
                        marketPrice, marketValue,
                        averageCost, unrealizedPNL,
                        realizedPNL, accountName):
        # Lock access to the position dicts. This is called in sub-thread and
        # can kick in at any time
        try:
            if not self._event_accdownload.is_set():  # 1st event seen
                position = Position(float(pos), float(averageCost))
                store_logger.debug(f"POSITIONS INITIAL: {self.positions}")
                # self.positions[contract.conId] = position
                with self._lock_pos:
                    self.positions.setdefault(contract.conId, position)
            else:
                with self._lock_pos:
                    position = self.positions[contract.conId]
                    fix_result = position.fix(float(pos), averageCost)
                store_logger.debug(f"POSITION UPDATE: {position}")
                if not fix_result:
                    err = ('The current calculated position and '
                        'the position reported by the broker do not match. '
                        'Operation can continue, but the trades '
                        'calculated in the strategy may be wrong')

                    self.notifs.put((err, (), {}))

                # Flag signal to broker at the end of account download
                # self.port_update = True
                self.broker.push_portupdate()
        except Exception as e:
            store_logger.exception(f"Exception: {e}")

    def getposition(self, contract, clone=False):
        # Lock access to the position dicts. This is called from main thread
        # and updates could be happening in the background
        with self._lock_pos:
            position = self.positions[contract.conId]
            if clone:
                return copy(position)

            return position

    @logibmsg
    def updateAccountValue(self, key, value, currency, accountName):
        # Lock access to the dicts where values are updated. This happens in a
        # sub-thread and could kick it at anytime
        try:
            value = float(value)
        except ValueError:
            value = value

        with self._lock_accupd:

            self.acc_upds[accountName][key][currency] = value

            if key == 'NetLiquidation':
                # NetLiquidationByCurrency and currency == 'BASE' is the same
                self.acc_value[accountName] = value
            elif key == 'CashBalance' and currency == 'BASE':
                self.acc_cash[accountName] = value
    
    def get_acc_values(self, account=None):
        '''Returns all account value infos sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the account infos can be returned to the calling client
        # if self.connected():
        #     self._event_accdownload.wait()
        # Lock access to acc_cash to avoid an event intefering
        if account is None:
            # wait for the managedAccount Messages
            # if self.connected():
            #     self._event_managed_accounts.wait()
            with self._lock_managed_acc:
                acc_count = len(self.managed_accounts)
                if acc_count == 1:
                    managed_account = self.managed_accounts[0]

            if acc_count == 0:
                with self._lock_accupd:
                    return self.acc_upds.copy()

            elif acc_count > 1:
                with self._lock_accupd:
                    return self.acc_upds.copy()

            # Only 1 account, fall through to return only 1
            account = managed_account

        try:
            with self._lock_accupd:
                return self.acc_upds[account].copy()
        except KeyError:
            pass

        with self._lock_accupd:
            return self.acc_upds.copy()

    def get_acc_value(self, account=None):
        '''Returns the net liquidation value sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the value can be returned to the calling client
        # Lock access to acc_cash to avoid an event intefering

        if account is None:
            with self._lock_managed_acc:
                acc_count = len(self.managed_accounts)
                if acc_count == 1:
                    managed_account = self.managed_accounts[0]

            if acc_count == 0:
                return float()
            elif acc_count > 1:
                return sum(self.acc_value.values())

            account = managed_account
            
        try:
            with self._lock_accupd:
                return self.acc_value[account]
        except KeyError:
            pass

        return float()

    def get_acc_cash(self, account=None):
        '''Returns the total cash value sent by TWS during regular updates
        Waits for at least 1 successful download

        If ``account`` is ``None`` then a dictionary with accounts as keys will
        be returned containing all accounts

        If account is specified or the system has only 1 account the dictionary
        corresponding to that account is returned
        '''
        # Wait for at least 1 account update download to have been finished
        # before the cash can be returned to the calling client
        # if self.connected():
        #     self._event_accdownload.wait()
        # Lock access to acc_cash to avoid an event intefering
        if account is None:
            # # wait for the managedAccount Messages
            # if self.connected():
            #     self._event_managed_accounts.wait()

            with self._lock_managed_acc:
                acc_count = len(self.managed_accounts)
                if acc_count == 1:
                    managed_account = self.managed_accounts[0]

            if acc_count == 0:
                return float()

            elif acc_count > 1:
                return sum(self.acc_cash.values())

            # Only 1 account, fall through to return only 1
            account = managed_account

        try:
            with self._lock_accupd:
                return self.acc_cash[account]
        except KeyError:
            pass

    def rebuild_after_reconnect(self):
        if not self.connected():
            return

        store_logger.info("Start rebuild the requests after reconnect....................")
        self._need_reconnect = False
        # new run thread
        self.apiThread = threading.Thread(target=self.conn.run, name="reconnect_ibapi_run", daemon=True)
        self.apiThread.start()

        # tell all datas the connection is reconnected
        for data in self.datas:
            if data is not None:
                # ibapi_wrapper.ibstore - ERROR - error: 1102 Connectivity between IBKR and Trader Workstation has been restored - data maintained. All data farms are connected: usfarm.nj; usfuture; cashfarm; usopt; usfarm; euhmds; fundfarm; ushmds; secdefnj.
                # use the same "restored"
                data.push_error("restored")

        # cancel all queue
        for q in self.ts:  # key: queue -> ticker
            q.put(None)

        with self._lock_q:
            self.qs = collections.OrderedDict()  # key: tickerId -> queues
            self.ts = collections.OrderedDict()  # key: queue -> tickerId
            self.iscash = dict()  # tickerIds from cash products (for ex: EUR.JPY)
            self.histexreq = dict()  # holds segmented historical requests
            self.histfmt = dict()  # holds datetimeformat for request
            self.histsend = dict()  # holds sessionend (data time) for request
            self.histtz = dict()  # holds sessionend (data time) for request
            self.realtz = dict()  # holds realtime bar timezone info

        self.reqAccountUpdates()
        self.reqPositions()

        # start data request again
        # the old threads will exit later
        self.startdatas()

        # tell all datas connection has been reseted
        for data in self.datas:
            if data is not None:
                data.push_error("restore_finished")
