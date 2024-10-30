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

import backtrader as bt
from backtrader.feed import DataBase
from backtrader import TimeFrame, date2num, num2date
from backtrader.utils.py3 import (integer_types, queue, string_types,
                                  with_metaclass)
from backtrader.metabase import MetaParams
from ibapi_wrapper import ibstore
import datetime
import pytz
import time
import tzlocal

import logging


CONTINUE = 0x11111111


class MetaIBData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaIBData, cls).__init__(name, bases, dct)

        # Register with the store
        ibstore.IBStore.DataCls = cls


class IBData(with_metaclass(MetaIBData, DataBase)):
    '''Interactive Brokers Data Feed.

    Supports the following contract specifications in parameter ``dataname``:

          - TICKER  # Stock type and SMART exchange
          - TICKER-STK  # Stock and SMART exchange
          - TICKER-STK-EXCHANGE  # Stock
          - TICKER-STK-EXCHANGE-CURRENCY  # Stock

          - TICKER-CFD  # CFD and SMART exchange
          - TICKER-CFD-EXCHANGE  # CFD
          - TICKER-CDF-EXCHANGE-CURRENCY  # Stock

          - TICKER-IND-EXCHANGE  # Index
          - TICKER-IND-EXCHANGE-CURRENCY  # Index

          - TICKER-YYYYMM-EXCHANGE  # Future
          - TICKER-YYYYMM-EXCHANGE-CURRENCY  # Future
          - TICKER-YYYYMM-EXCHANGE-CURRENCY-MULT  # Future
          - TICKER-FUT-EXCHANGE-CURRENCY-YYYYMM-MULT # Future

          - TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT  # FOP
          - TICKER-YYYYMM-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # FOP
          - TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT # FOP
          - TICKER-FOP-EXCHANGE-CURRENCY-YYYYMM-STRIKE-RIGHT-MULT # FOP

          - CUR1.CUR2-CASH-IDEALPRO  # Forex

          - TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT  # OPT
          - TICKER-YYYYMMDD-EXCHANGE-CURRENCY-STRIKE-RIGHT-MULT  # OPT
          - TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT # OPT
          - TICKER-OPT-EXCHANGE-CURRENCY-YYYYMMDD-STRIKE-RIGHT-MULT # OPT

    Params:

      - ``sectype`` (default: ``STK``)

        Default value to apply as *security type* if not provided in the
        ``dataname`` specification

      - ``exchange`` (default: ``SMART``)

        Default value to apply as *exchange* if not provided in the
        ``dataname`` specification

      - ``primaryExchange`` (default: ``None``)

        For certain smart-routed stock contracts that have the same symbol, 
        currency and exchange, you would also need to specify the primary 
        exchange attribute to uniquely define the contract. This should be 
        defined as the native exchange of a contract

      - ``right`` (default: ``None``)

        Warrants, like options, require an expiration date, a right, 
        a strike and an optional multiplier.

      - ``strike`` (default: ``None``)

        Warrants, like options, require an expiration date, a right, 
        a strike and an optional multiplier.

      - ``expiry`` (default: ``None``)

        Warrants, like options, require an expiration date, a right, 
        a strike and an optional multiplier.
        In this case expiry is 'lastTradeDateOrContractMonth'

      - ``currency`` (default: ``''``)

        Default value to apply as *currency* if not provided in the
        ``dataname`` specification

      - ``multiplier`` (default: ``None``)

        Occasionally, you can expect to have more than a single future 
        contract for the same underlying with the same expiry. To rule 
        out the ambiguity, the contract's multiplier can be given

      - ``tradingClass`` (default: ``None``)

        It is not unusual to find many option contracts with an almost identical 
        description (i.e. underlying symbol, strike, last trading date, 
        multiplier, etc.). Adding more details such as the trading class will help

      - ``localSymbol`` (default: ``None``)

        Warrants, like options, require an expiration date, a right, a strike and 
        a multiplier. For some warrants it will be necessary to define a 
        localSymbol or conId to uniquely identify the contract

      - ``historical`` (default: ``False``)

        If set to ``True`` the data feed will stop after doing the first
        download of data.

        The standard data feed parameters ``fromdate`` and ``todate`` will be
        used as reference.

        The data feed will make multiple requests if the requested duration is
        larger than the one allowed by IB given the timeframe/compression
        chosen for the data.

      - ``what`` (default: ``None``)

        If ``None`` the default for different assets types will be used for
        historical data requests:

          - 'BID' for CASH assets
          - 'TRADES' for any other

        Use 'ASK' for the Ask quote of cash assets
        
        Check the IB API docs if another value is wished
        (TRADES,MIDPOINT,BID,ASK,BID_ASK,ADJUSTED_LAST,HISTORICAL_VOLATILITY,
         OPTION_IMPLIED_VOLATILITY, REBATE_RATE, FEE_RATE,
         YIELD_BID, YIELD_ASK, YIELD_BID_ASK, YIELD_LAST)

      - ``rtbar`` (default: ``False``)

        If ``True`` the ``5 Seconds Realtime bars`` provided by Interactive
        Brokers will be used as the smalles tick. According to the
        documentation they correspond to real-time values (once collated and
        curated by IB)

        If ``False`` then the ``RTVolume`` prices will be used, which are based
        on receiving ticks. In the case of ``CASH`` assets (like for example
        EUR.JPY) ``RTVolume`` will always be used and from it the ``bid`` price
        (industry de-facto standard with IB according to the literature
        scattered over the Internet)

        Even if set to ``True``, if the data is resampled/kept to a
        timeframe/compression below Seconds/5, no real time bars will be used,
        because IB doesn't serve them below that level

      - ``qcheck`` (default: ``0.5``)

        Time in seconds to wake up if no data is received to give a chance to
        resample/replay packets properly and pass notifications up the chain

      - ``backfill_start`` (default: ``True``)

        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.

      - ``backfill`` (default: ``True``)

        Perform backfilling after a disconnection/reconnection cycle. The gap
        duration will be used to download the smallest possible amount of data

      - ``backfill_from`` (default: ``None``)

        An additional data source can be passed to do an initial layer of
        backfilling. Once the data source is depleted and if requested,
        backfilling from IB will take place. This is ideally meant to backfill
        from already stored sources like a file on disk, but not limited to.

      - ``latethrough`` (default: ``False``)

        If the data source is resampled/replayed, some ticks may come in too
        late for the already delivered resampled/replayed bar. If this is
        ``True`` those ticks will bet let through in any case.

        Check the Resampler documentation to see who to take those ticks into
        account.

        This can happen especially if ``timeoffset`` is set to ``False``  in
        the ``IBStore`` instance and the TWS server time is not in sync with
        that of the local computer

      - ``tradename`` (default: ``None``)
        Useful for some specific cases like ``CFD`` in which prices are offered
        by one asset and trading happens in a different onel

        - SPY-STK-SMART-USD -> SP500 ETF (will be specified as ``dataname``)

        - SPY-CFD-SMART-USD -> which is the corresponding CFD which offers not
          price tracking but in this case will be the trading asset (specified
          as ``tradename``)

    The default values in the params are the to allow things like ```TICKER``,
    to which the parameter ``sectype`` (default: ``STK``) and ``exchange``
    (default: ``SMART``) are applied.

    Some assets like ``AAPL`` need full specification including ``currency``
    (default: '') whereas others like ``TWTR`` can be simply passed as it is.

      - ``AAPL-STK-SMART-USD`` would be the full specification for dataname

        Or else: ``IBData`` as ``IBData(dataname='AAPL', currency='USD')``
        which uses the default values (``STK`` and ``SMART``) and overrides
        the currency to be ``USD``
    '''
    params = (
        ('secType', 'STK'),  # usual industry value
        ('exchange', 'SMART'),  # usual industry value
        ('primaryExchange', None),  # native exchange of the contract
        ('right', None),  # Option or Warrant Call('C') or Put('P')
        ('strike', None),  # Future, Option or Warrant strike price
        ('multiplier', None),  # Future, Option or Warrant multiplier
        ('expiry', None),  # Future, Option or Warrant lastTradeDateOrContractMonth date 
        ('currency', ''),  # currency for the contract
        ('localSymbol', None),  # Warrant localSymbol override
        ('rtbar', False),  # use RealTime 5 seconds bars
        ('historical', False),  # only historical download
        ('what', None),  # historical - what to show
        ('useRTH', False),  # historical - download only Regular Trading Hours
        ('qcheck', 0.5),  # timeout in seconds (float) to check for events
        ('backfill_start', True),  # do backfilling at the start
        ('backfill', True),  # do backfilling when reconnecting
        ('backfill_from', None),  # additional data source to do backfill from
        ('latethrough', False),  # let late samples through
        ('tradename', None),  # use a different asset as order target
        ('numberOfTicks', 1000),  # Number of distinct data points. Max is 1000 per request.
        ('ignoreSize', False),  # Omit updates that reflect only changes in size, and not price. Applicable to Bid_Ask data requests.
        ('rth_duration', None),     # session last time
        ('use_date_split', False),  # split date when date range exceeds max duration
        ('ignore_incomplete', False),  # ignore incomplete data
        ('ignore_fetcherror', False),  # ignore fetch error
        ('live_retry_times', 3),       # when fetch backfill data failed, retry maximinum times
    )

    _store = ibstore.IBStore

    # Minimum size supported by real-time bars
    RTBAR_MINSIZE = (TimeFrame.Seconds, 5)

    # States for the Finite State Machine in _load
    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)

    def _timeoffset(self):
        return self.ib.timeoffset()

    def _gettz(self):
        # If no object has been provided by the user and a timezone can be
        # found via contractdtails, then try to get it from pytz, which may or
        # may not be available.

        # The timezone specifications returned by TWS seem to be abbreviations
        # understood by pytz, but the full list which TWS may return is not
        # documented and one of the abbreviations may fail
        tzstr = isinstance(self.p.tz, string_types)
        if self.p.tz is not None and not tzstr:
            return bt.utils.date.Localizer(self.p.tz)

        if self.contractdetails is None:
            return None  # nothing can be done

        try:
            import pytz  # keep the import very local
        except ImportError:
            return None  # nothing can be done

        tzs = self.p.tz if tzstr else self.contractdetails.timeZoneId

        if tzs == 'CST':  # reported by TWS, not compatible with pytz. patch it
            tzs = 'CST6CDT'

        try:
            tz = pytz.timezone(tzs)
        except pytz.UnknownTimeZoneError:
            return None  # nothing can be done

        # contractdetails there, import ok, timezone found, return it
        return tz

    def islive(self):
        '''Returns ``True`` to notify ``Cerebro`` that preloading and runonce
        should be deactivated'''
        return not self.p.historical

    def __init__(self, **kwargs):
        self.ib = self._store(**kwargs)
        self.precontract = self.parsecontract(self.p.dataname)
        self.pretradecontract = self.parsecontract(self.p.tradename)
        self.qerror = queue.Queue()
        self.init_logger()
        self.init_trade_hours_data()
        self._retry_fetch_method = None
        self._lose_connection = False
        self._lose_connection_time = None
        self._live_retry_times = None

    def skip_data(self):
        if self._lose_connection:
            self.logger.info(f"Skip {self._name} data because of losing connection")
            return True
        else:
            return False

    def init_trade_hours_data(self):
        self._liquid_hours = {}
        self._trade_ours = {}

    def init_logger(self):
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            stream_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            stream_handler.setFormatter(formatter)
            stream_handler.setLevel(logging.INFO)
            self.logger.addHandler(stream_handler)
            self.logger.setLevel(logging.INFO)

    def setenvironment(self, env):
        '''Receives an environment (cerebro) and passes it over to the store it
        belongs to'''
        super(IBData, self).setenvironment(env)
        env.addstore(self.ib)

    def parsecontract(self, dataname):
        '''Parses dataname generates a default contract'''
        # Set defaults for optional tokens in the ticker string
        if dataname is None:
            return None

        exch = self.p.exchange
        primaryExch = self.p.primaryExchange
        curr = self.p.currency
        expiry = self.p.expiry
        strike = self.p.strike
        right = self.p.right
        mult = self.p.multiplier
        localSymbol = self.p.localSymbol

        # split the ticker string
        tokens = iter(dataname.split('-'))

        # Symbol and security type are compulsory
        symbol = next(tokens)
        try:
            sectype = next(tokens)
        except StopIteration:
            sectype = self.p.secType

        # security type can be an expiration date
        if sectype.isdigit():
            expiry = sectype  # save the expiration ate

            if len(sectype) == 6:  # YYYYMM
                sectype = 'FUT'
            else:  # Assume OPTIONS - YYYYMMDD
                sectype = 'OPT'

        if sectype == 'CASH':  # need to address currency for Forex
            symbol, curr = symbol.split('.')

        # See if the optional tokens were provided
        try:
            exch = next(tokens)  # on exception it will be the default
            curr = next(tokens)  # on exception it will be the default

            if sectype == 'FUT':
                if not expiry:
                    expiry = next(tokens)
                mult = next(tokens)

                # Try to see if this is FOP - Futures on OPTIONS
                right = next(tokens)
                # if still here this is a FOP and not a FUT
                sectype = 'FOP'
                strike, mult = float(mult), ''  # assign to strike and void

                mult = next(tokens)  # try again to see if there is any

            elif sectype == 'OPT':
                if not expiry:
                    expiry = next(tokens)
                strike = float(next(tokens))  # on exception - default
                right = next(tokens)  # on exception it will be the default

                mult = next(tokens)  # ?? no harm in any case

        except StopIteration:
            pass

        # Make the initial contract
        precon = self.ib.makecontract(
            symbol=symbol, sectype=sectype, exch=exch, curr=curr,
            expiry=expiry, strike=strike, right=right, mult=mult, 
            primaryExch=primaryExch, localSymbol=localSymbol)

        return precon

    def start(self):
        '''Starts the IB connecction and gets the real contract and
        contractdetails if it exists'''
        super(IBData, self).start()
        # Kickstart store and get queue to wait on
        self.qlive = self.ib.start(data=self)
        self.qhist = None

        self._usertvol = not self.p.rtbar
        tfcomp = (self._timeframe, self._compression)
        if tfcomp < self.RTBAR_MINSIZE:
            # Requested timeframe/compression not supported by rtbars
            self._usertvol = True

        self.contract = None
        self.contractdetails = None
        self.tradecontract = None
        self.tradecontractdetails = None
        self.init_trade_hours_data()

        if self.p.backfill_from is not None:
            self._state = self._ST_FROM
            self.p.backfill_from.setenvironment(self._env)
            self.p.backfill_from._start()
        else:
            self._state = self._ST_START  # initial state for _load
        self._statelivereconn = False  # if reconnecting in live state
        self._subcription_valid = False  # subscription state
        self._historical_get_data = False # historical data started status
        self._historical_get_date_time = None
        self._historical_ended = False  # historical data ended status
        self._storedmsg = dict()  # keep pending live message (under None)

        if not self.ib.connected():
            return

        self.put_notification(self.CONNECTED)
        # get real contract details with real conId (contractId)
        cds = self.ib.getContractDetails(self.precontract, maxcount=1)
        if cds is not None:
            cdetails = cds[0]
            self.contract = cdetails.contract
            self.contractdetails = cdetails
            self.init_trade_hours_data()
        else:
            # no contract can be found (or many)
            self.put_notification(self.DISCONNECTED)
            return

        if self.pretradecontract is None:
            # no different trading asset - default to standard asset
            self.tradecontract = self.contract
            self.tradecontractdetails = self.contractdetails
        else:
            # different target asset (typical of some CDS products)
            # use other set of details
            cds = self.ib.getContractDetails(self.pretradecontract, maxcount=1)
            if cds is not None:
                cdetails = cds[0]
                self.tradecontract = cdetails.contract
                self.tradecontractdetails = cdetails
            else:
                # no contract can be found (or many)
                self.put_notification(self.DISCONNECTED)
                return

        if self._state == self._ST_START:
            self._start_finish()  # to finish initialization
            self._st_start()

    def stop(self):
        '''Stops and tells the store to stop'''
        super(IBData, self).stop()
        self.ib.stop()

    def reqdata(self):
        '''request real-time data. checks cash vs non-cash) and param useRT'''
        if self.contract is None or self._subcription_valid:
            return

        if not self.islive():
            return

        if self._usertvol and self._timeframe != bt.TimeFrame.Ticks:
            self.qlive = self.ib.reqMktData(self.contract, self.p.what)
        elif self._usertvol and self._timeframe == bt.TimeFrame.Ticks:
            self.qlive = self.ib.reqTickByTickData(self.contract, self.p.what)
        else:
            self.qlive = self.ib.reqRealTimeBars(self.contract, what = self.p.what, tz=self._gettz())

        self._subcription_valid = True
        self.logger.info(f"Start real-time data request: {self.contract.symbol}")
        return self.qlive

    def canceldata(self):
        '''Cancels Market Data subscription, checking asset type and rtbar'''
        if self.contract is None:
            return

        if not self.islive():
            return

        if self._usertvol and self._timeframe != bt.TimeFrame.Ticks:
            self.ib.cancelMktData(self.qlive)
        elif self._usertvol and self._timeframe == bt.TimeFrame.Ticks:
            self.ib.cancelTickByTickData(self.qlive)
        else:
            self.ib.cancelRealTimeBars(self.qlive)

        self._subcription_valid = False
        self.logger.info(f"Cancel real-time data request: {self.contract.symbol}")

    def haslivedata(self):
        return bool(self._storedmsg or self.qlive)

    def do_qcheck(self, onoff, qlapse):
        # if onoff is True the data will wait p.qcheck for incoming live data
        # on its queue.
        qwait = self.p.qcheck
        qwait = max(0.0, qwait - qlapse)
        self._qcheck = qwait

    def _check_and_reset_live_historical_data_retry(self):
        if self.p.historical:
            return

        if self._live_retry_times is not None:
            self._live_retry_times = None
            self._statelivereconn = False
            self.logger.info(f"data({self._name}) fetch live historical data success, reset the retry times")

    def _fix_fetch_todate(self):
        local_timezone = tzlocal.get_localzone_name()
        local_dt = pytz.timezone(local_timezone).localize(datetime.datetime.now())
        sleep = 0

        if self._timeframe in [bt.TimeFrame.Seconds, bt.TimeFrame.Minutes]:
            compitable_tz = local_dt + datetime.timedelta(minutes=1)
            # we cannot substract the time like day data
            # because the minutes data is huge data, we cannot gurantee that the market is closed when substract the time
            # if that, we will lose market data
            # so when there is a minute or second data error, just wait the market open
            # sleep one minute and move the end data
            if self._lose_connection_time:
                lose_delta = local_dt - self._lose_connection_time
                self._lose_connection_time = None
                self.logger.info(f"Fix the fetch todate in disconnection, todate is {self.p.todate} lose delta is {lose_delta}")
                self.p.todate += lose_delta
            else:
                self.p.todate += datetime.timedelta(minutes=1)
            sleep = 60
            if self.p.todate.tzinfo is None:
                self.p.todate = self._gettz().localize(self.p.todate)
            if self.p.todate > compitable_tz:
                self.p.todate = compitable_tz
        else:
            if self._retry_fetch_method is None:
                if self.p.todate.date() >= local_dt.date():
                    self._retry_fetch_method = 'sub'
                else:
                    self._retry_fetch_method = 'add'

            if self._retry_fetch_method == 'sub':
                self.p.todate = self.p.todate - datetime.timedelta(days=1)
                if self.p.todate < local_dt - datetime.timedelta(days=29):
                    self._retry_fetch_method = None
            else:
                self.p.todate = self.p.todate + datetime.timedelta(days=1)
                if self.p.todate > local_dt + datetime.timedelta(days=1):
                    self._retry_fetch_method = None

        # set the request time
        self.todate = self.date2num(self.p.todate)
        return sleep

    def _load_live_data(self):
        try:
            msg = (self._storedmsg.pop(None, None) or
                    self.qlive.get(timeout=self._qcheck))
        except queue.Empty:
            return None

        if msg is None:  # Conn broken during historical/backfilling
            self._subcription_valid = False
            self.put_notification(self.CONNBROKEN)
            self.ib.set_losing_data(True)
            self._statelivereconn = self.p.backfill
            return CONTINUE

        if msg == -354:
            self.ib.set_losing_data(True)
            self.put_notification(self.NOTSUBSCRIBED)
            return False

        elif isinstance(msg, integer_types):
            # Unexpected notification for historical data skip it
            # May be a "not connected not yet processed"
            self.put_notification(self.UNKNOWN, msg)
            return CONTINUE

        self.ib.set_losing_data(False)

        # Process the message according to expected return type
        if not self._statelivereconn:
            if self._laststatus != self.LIVE:
                if self.qlive.qsize() <= 1:  # very short live queue
                    self.put_notification(self.LIVE)

            if self._usertvol and self._timeframe != bt.TimeFrame.Ticks:
                ret = self._load_rtdata(msg)
            elif self._usertvol and self._timeframe == bt.TimeFrame.Ticks:
                ret = self._load_rtticks(msg)
            else:
                ret = self._load_rtbar(msg)
            if ret:
                return True

            # could not load bar ... go and get new one
            return CONTINUE

        # Fall through to processing reconnect - try to backfill
        self._storedmsg[None] = msg  # keep the msg

        # else do a backfill
        if self._laststatus != self.DELAYED:
            self.put_notification(self.DELAYED)

        dtend = None
        if len(self) > 1:
            # len == 1 ... forwarded for the 1st time
            # get begin date in utc-like format like msg.datetime
            dtbegin = num2date(self.datetime[-1])
            dtbegin = pytz.utc.localize(dtbegin)
        elif self.fromdate > float('-inf'):
            dtbegin = num2date(self.fromdate)
            dtbegin = pytz.utc.localize(dtbegin)
        else:  # 1st bar and no begin set
            # passing None to fetch max possible in 1 request
            dtbegin = None

        dtend = msg.datetime if self._usertvol else msg.time
        self.p.todate = dtend
        if dtend.tzinfo is not None:
            dtend = dtend.astimezone(pytz.utc)

        if self._timeframe != bt.TimeFrame.Ticks:
            self._historical_ended = False
            self._historical_get_data = False
            self.qhist = self.ib.reqHistoricalDataEx(
                contract=self.contract, enddate=dtend, begindate=dtbegin,
                timeframe=self._timeframe, compression=self._compression,
                what=self.p.what, useRTH=self.p.useRTH, tz=self._tz,
                sessionend=self.p.sessionend, useSplit=self.p.use_date_split)
        else:
            # dtend = num2date(dtend)
            self._historical_ended = False
            self._historical_get_data = False
            self.qhist = self.ib.reqHistoricalTicksEx(
                contract=self.contract, enddate=dtend,
                what=self.p.what, useRTH=self.p.useRTH, tz=self._tz,
                )

        self._state = self._ST_HISTORBACK
        self._statelivereconn = False  # no longer in live
        return CONTINUE

    def _load_histrial_data(self):
        if self._historical_ended:
            return False

        try:
            # There is no historical data sometimes, especially when start the data during the closed market
            if not self._historical_get_data:
                msg = self.qhist.get(timeout=self._qcheck)
            else:
                msg = self.qhist.get()
        except queue.Empty:
            if self.p.historical:  # only historical
                if self._historical_get_date_time is None:
                    self.logger.warning(f"We didn't get historical data {self._name}, consider to set the self.p.qcheck from {self._qcheck} to 0.0 to accelerate the process.")
                    self._historical_get_date_time = datetime.datetime.now()

                if datetime.datetime.now() - self._historical_get_date_time > datetime.timedelta(seconds=60):
                    self.logger.warning(f"We didn't get historical data {self._name} for 60 seconds, consider to set the self.p.qcheck from {self._qcheck} to 0.0 to accelerate the process.")
                    self._historical_get_date_time = datetime.datetime.now()
                # self.put_notification(self.DISCONNECTED)
                return None  # end of historical

            # Live is also wished - go for it
            self._state = self._ST_LIVE
            self._check_and_reset_live_historical_data_retry()
            return CONTINUE

        self._historical_get_data = True
        self._historical_get_date_time = datetime.datetime.now()

        if msg is None:  # Conn broken during historical/backfilling
            # Situation not managed. Simply bail out
            self._subcription_valid = False
            self._historical_ended = True
            # check the live mode
            if self.p.historical:
                self.put_notification(self.DISCONNECTED)
                return False
            else:
                # return back to Live mode
                self._state = self._ST_LIVE
                self._check_and_reset_live_historical_data_retry()
                return CONTINUE

        elif msg == "WaitSplit":
            self._historical_get_data = False
            self._historical_get_date_time = None
            self.logger.info(f"Receive WaitSplit Msg, qcheck is {self._qcheck}")
            return CONTINUE

        elif msg in [162, 320, 321, 322]:
            if self.p.historical:
                if not self.p.ignore_fetcherror:
                    # fetch the data again
                    sleep_time = self._fix_fetch_todate()
                    self.logger.info(f"Try again to fetch historical data, qcheck is {self._qcheck}, to date is {self.p.todate} timeframe {self._timeframe} {self._retry_fetch_method} sleep {sleep_time}") 
                    time.sleep(sleep_time)
                    self._st_start()
                    self._historical_get_data = False
                    self._historical_get_date_time = None
                    return CONTINUE
                else:
                    self.logger.info(f"Stop fetching historical data, qcheck is {self._qcheck}")
                    # stop data
                    self._subcription_valid = False
                    self._historical_ended = True
                    self.put_notification(self.DISCONNECTED)
                    return False
            else:
                # try to fetch the data again
                if self._live_retry_times is None:
                    self._live_retry_times = self.p.live_retry_times

                if self._live_retry_times > 0:
                    self._live_retry_times -= 1
                    self._statelivereconn = self.p.backfill
                    self.logger.info(f"Try again to fetch live data({self._name}), qcheck is {self._qcheck}, retry times {self._live_retry_times}")
                    time.sleep(self._qcheck)
                else:
                    self.logger.info(f"Fetch live historical data({self._name}) failed, ignore the data")
                    self._statelivereconn = False
                return CONTINUE

        elif msg == -354:  # Data not subscribed
            self._subcription_valid = False
            self.put_notification(self.NOTSUBSCRIBED)
            return False

        elif msg == -420:  # No permissions for the data
            self._subcription_valid = False
            self.put_notification(self.NOTSUBSCRIBED)
            return False

        elif isinstance(msg, integer_types):
            # Unexpected notification for historical data skip it
            # May be a "not connected not yet processed"
            self.put_notification(self.UNKNOWN, msg)
            self.logger.info(f"Receive unknown error msg {msg}")
            return CONTINUE

        self.ib.set_losing_data(False)
        self._check_and_reset_live_historical_data_retry()

        if msg.date is not None:
            if self._timeframe == bt.TimeFrame.Ticks:
                if self._load_rtticks(msg, hist=True):
                    return True
            else:
                if self._load_rtbar(msg, hist=True):
                    return True  # loading worked

            # the date is from overlapping historical request
            return CONTINUE

        # End of histdata
        if self.p.historical:  # only historical
            self.put_notification(self.DISCONNECTED)
            return False  # end of historical

        # Live is also required - go for it
        self._state = self._ST_LIVE
        return CONTINUE

    def _load_from_data(self):
        if not self.p.backfill_from.next():
            # additional data source is consumed
            self._state = self._ST_START
            return CONTINUE

        # copy lines of the same name
        for alias in self.lines.getlinealiases():
            lsrc = getattr(self.p.backfill_from.lines, alias)
            ldst = getattr(self.lines, alias)

            ldst[0] = lsrc[0]

        return True

    def _load(self):
        self._process_errors()

        if self._lose_connection:
            self.logger.info(f"Fetch data lose connection, {self._name}, todate is {self.p.todate} sleep for {self.p.qcheck}")
            time.sleep(self.p.qcheck)
            return None

        if self.contract is None or self._state == self._ST_OVER:
            return False  # nothing can be done

        while True:
            if self._state == self._ST_LIVE:
                result = self._load_live_data()
            elif self._state == self._ST_HISTORBACK:
                result = self._load_histrial_data()
            elif self._state == self._ST_FROM:
                result = self._load_from_data()
            elif self._state == self._ST_START:
                if not self._st_start():
                    return False
                else:
                    result = CONTINUE

            if result == CONTINUE:
                continue
            else:
                return result

    def _st_start(self):
        if self.p.historical:
            self.put_notification(self.DELAYED)
            dtend = None
            if self.todate < float('inf'):
                dtend = num2date(self.todate)

            dtbegin = None
            if self.fromdate > float('-inf'):
                dtbegin = num2date(self.fromdate)

            if self._timeframe == bt.TimeFrame.Ticks:
                self._historical_ended = False
                self._historical_get_data = False
                self.qhist = self.ib.reqHistoricalTicksEx(
                    contract=self.contract, enddate=dtend, begindate=dtbegin,
                    what=self.p.what, useRTH=self.p.useRTH, tz=self._tz)
            else:
                self._historical_ended = False
                self._historical_get_data = False
                self.qhist = self.ib.reqHistoricalDataEx(
                    contract=self.contract, enddate=dtend, begindate=dtbegin,
                    timeframe=self._timeframe, compression=self._compression,
                    what=self.p.what, useRTH=self.p.useRTH, tz=self._tz,
                    sessionend=self.p.sessionend, useSplit=self.p.use_date_split)

            self._state = self._ST_HISTORBACK
            return True  # continue before

        # Live is requested
        if not self.ib.reconnect(resub=True):
            self.put_notification(self.DISCONNECTED)
            self._state = self._ST_OVER
            return False  # failed - was so

        self._statelivereconn = self.p.backfill_start
        if self.p.backfill_start:
            self.put_notification(self.DELAYED)

        self._state = self._ST_LIVE
        return True  # no return before - implicit continue

    def _load_rtbar(self, rtbar, hist=False):
        # A complete 5 second bar made of real-time ticks is delivered and
        # contains open/high/low/close/volume prices
        # The historical data has the same data but with 'date' instead of
        # 'time' for datetime
        dt = date2num(rtbar.time if not hist else rtbar.date)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered

        # ignore incomplete data, for example now is 2024-07-24 13:00:00
        # but the data is 2024-07-24 16:00:00 for the daily data
        # if set ignore_incomplete to True, the data will be ignored
        if self.p.ignore_incomplete:
            if self._timeoffset:
                now = datetime.datetime.now(self._gettz()) + self._timeoffset()
            else:
                now = datetime.datetime.now(self._gettz())

            if now + datetime.timedelta(seconds=30) < rtbar.date:
                return False

        self.lines.datetime[0] = dt
        # Put the tick into the bar
        self.lines.open[0] = rtbar.open
        self.lines.high[0] = rtbar.high
        self.lines.low[0] = rtbar.low
        self.lines.close[0] = rtbar.close
        self.lines.volume[0] = rtbar.volume
        self.lines.openinterest[0] = 0

        return True

    def _load_rtdata(self, rtdata):
        if rtdata.is_valid is False:
            return False

        dt = date2num(rtdata.datetime)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered

        self.lines.datetime[0] = dt

        # get the data type, only use the high, low, close, volume now
        if rtdata.field == 'LAST_PRICE':
            self.lines.close[0] = rtdata.value

            self.lines.open[0] = float('-inf')
            self.lines.high[0] = float('-inf')
            self.lines.low[0] = float('-inf')
            self.lines.volume[0] = float('-inf')
            self.lines.openinterest[0] = float('-inf')
        elif rtdata.field == 'HIGH':
            self.lines.high[0] = rtdata.value

            self.lines.close[0] = float('-inf')
            self.lines.open[0] = float('-inf')
            self.lines.low[0] = float('-inf')
            self.lines.volume[0] = float('-inf')
            self.lines.openinterest[0] = float('-inf')
        elif rtdata.field == 'LOW':
            self.lines.low[0] = rtdata.value

            self.lines.close[0] = float('-inf')
            self.lines.open[0] = float('-inf')
            self.lines.high[0] = float('-inf')
            self.lines.volume[0] = float('-inf')
            self.lines.openinterest[0] = float('-inf')

        elif rtdata.field == 'VOLUME':
            self.lines.volume[0] = rtdata.value

            self.lines.close[0] = float('-inf')
            self.lines.open[0] = float('-inf')
            self.lines.high[0] = float('-inf')
            self.lines.low[0] = float('-inf')
            self.lines.openinterest[0] = float('-inf')

        else:
            return False

        return True

    def _load_rtticks(self, tick, hist=False):

        dt = date2num(tick.datetime if not hist else tick.date)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered

        self.lines.datetime[0] = dt

        if tick.dataType == 'RT_TICK_MIDPOINT':
            self.lines.close[0] = tick.midPoint
        elif tick.dataType == 'RT_TICK_BID_ASK':
            self.lines.open[0] = tick.bidPrice
            self.lines.close[0] = tick.askPrice
            self.lines.volume[0] = tick.bidSize
            self.lines.openinterest[0] = tick.askSize
        elif tick.dataType == 'RT_TICK_LAST':
            self.lines.close[0] = tick.price
            self.lines.volume[0] = tick.size

        return True

    def _process_errors(self):
        if self.qerror.empty():
            return

        while not self.qerror.empty():
            msg = self.qerror.get()

            if msg.errorCode in [502, 504, 1102, 1101, 10225]:
                self._subcription_valid = False
                self.put_notification(self.CONNBROKEN)
                self._statelivereconn = self.p.backfill
                self._lose_connection = True
                if self._lose_connection_time is None:
                    self._lose_connection_time = pytz.timezone(tzlocal.get_localzone_name()).localize(datetime.datetime.now())
                self.logger.info(f"Receive connection error {msg.errorCode}, set the connection to False {self._name}")
            else:
                self.logger.info(f"Receive error message: {msg.errorCode} {msg.errorMsg} {self._name} pass throuhg")
                pass

    def push_error(self, msg):
        self.logger.info(f"Push error message: {msg} {self._name}")
        if msg == "reconnected":
            self._lose_connection = False
            self.put_notification(self.CONNECTED)
            if self.p.historical:
                if not self._historical_ended:
                    self._state = self._ST_START
            else:
                self._state = self._ST_START
            self.qhist = None
            self.qlive = None
            self._subcription_valid = False
            self._storedmsg = dict()
            self.logger.info(f"Receive reconnected message, set the connection to {self._lose_connection} {self._name}")
        elif msg == "reconnect_finished":
            if self._state == self._ST_START:
                self._st_start()
                self.logger.info(f"Receive reconnect finished message, start the data {self._name}")
        else:
            self.qerror.put(msg)

    def _parse_trading_hours(self, hours_str):
        sessions = {}
        for part in hours_str.split(";"):
            if "-" in part:
                # 20240729:0930-20240729:1600
                start, end = part.split("-")
                date, start_time = start.split(":")
                _, end_time = end.split(":")
                start_dt = self._tz.localize(datetime.datetime.strptime(f"{date} {start_time}", "%Y%m%d %H%M"))
                end_dt = self._tz.localize(datetime.datetime.strptime(f"{date} {end_time}", "%Y%m%d %H%M"))
                sessions[date] = {"start":start_dt, "end":end_dt}
            elif ":" in part:
                date, rest = part.split(":")
                if rest.lower() == "closed":
                    sessions[date] = {"start": None, "end": None}
                else:
                    raise RuntimeError(f"Cannot parse trading hours: {hours_str} that is not closed")
            else:
                raise RuntimeError(f"Cannot parse trading hours: {hours_str}")
        return sessions

    def get_liquid_hours(self):
        """Definition: Liquid hours represent the time period when there is typically higher trading volume and liquidity for the contract. This means that there are usually more buyers and sellers active in the market during these hours, leading to tighter bid-ask spreads and potentially more efficient price execution.
Usage: These hours are often considered the primary trading period for the instrument and are usually the most relevant for active traders or those seeking to execute larger orders.
Example: For a US stock, liquid hours might be 9:30 AM to 4:00 PM ET, which is the regular trading session for most US equities.
"""
        if len(self._liquid_hours) > 0:
            return self._liquid_hours

        if self.contractdetails is None:
            return self._liquid_hours

        liquid_hours = self.contractdetails.liquidHours
        # example value: '20240727:CLOSED;20240728:CLOSED;20240729:0930-20240729:1600;20240730:0930-20240730:1600;20240731:0930-20240731:1600;20240801:0930-20240801:1600'
        self._liquid_hours = self._parse_trading_hours(liquid_hours)
        return self._liquid_hours

    def get_trade_hours(self):
        """Definition: Trading hours represent the overall period when the contract is available for trading. This can include the liquid hours as well as any extended or pre-market/post-market sessions where trading is allowed but may be less active.
Usage: These hours provide a broader view of when the contract is tradeable and are useful for understanding the full range of trading opportunities, especially for those interested in extended or off-hours trading.
Example: For a US stock, trading hours might be 4:00 AM to 8:00 PM ET, encompassing the pre-market, regular session, and after-hours trading.
        """
        if len(self._trade_ours) > 0:
            return self._trade_ours

        if self.contractdetails is None:
            return self._trade_ours

        trade_hours = self.contractdetails.tradingHours
        self._trade_ours = self._parse_trading_hours(trade_hours)
        return self._trade_ours
