"""
Test suite for IB API wrapper fixes
Tests commission report crash fix by calling actual business logic
"""
import pytest
from unittest.mock import Mock
from ibapi_wrapper.ibbroker import IBBroker


@pytest.fixture
def mock_execution():
    """Create mock execution object with realistic IB API structure"""
    execution = Mock()
    execution.execId = "test_exec_123"
    execution.orderId = 1001  
    execution.shares = 100
    execution.side = "BOT"
    execution.price = 150.0
    execution.cumQty = 100
    execution.time = "20250630 10:30:00"
    return execution


@pytest.fixture  
def mock_commission_report():
    """Create mock commission report object with realistic IB API structure"""
    commission_report = Mock()
    commission_report.execId = "test_exec_123"  # Must match execution
    commission_report.commissionAndFees = 5.25
    commission_report.realizedPNL = 100.0
    return commission_report


@pytest.fixture
def ib_broker_with_position_mock():
    """Create IBBroker with minimal mocking for position handling"""
    broker = IBBroker()
    broker.executions = {}
    broker.orderbyid = {}
    broker.ordstatus = {}
    
    # Mock position-related functionality
    mock_position = Mock()
    mock_position.price = 140.0
    mock_position.update.return_value = (100, 150.0, 0, 100)  # psize, pprice, opened, closed
    broker.getposition = Mock(return_value=mock_position)
    
    # Mock logger to verify warnings
    broker.logger = Mock()
    
    return broker


def test_push_commissionreport_handles_none_comminfo_no_crash(
    ib_broker_with_position_mock, mock_execution, mock_commission_report
):
    """
    BUSINESS LOGIC TEST: Verify push_commissionreport() handles None comminfo without crashing
    and passes correct commission values (0.0) to order.execute()
    """
    # Arrange - Setup IBBroker internal state for realistic execution flow
    broker = ib_broker_with_position_mock
    broker.executions[mock_execution.execId] = mock_execution
    
    # Create order with None comminfo (the crash scenario)
    order = Mock()
    order.comminfo = None  # This was causing the crash
    order.data = Mock()
    order.data.close = [150.0]  # Mock close price access
    order.execute = Mock()
    
    broker.orderbyid[mock_execution.orderId] = order
    broker.ordstatus[mock_execution.orderId] = {mock_execution.cumQty: Mock()}
    
    # Act - Call the actual business method that was crashing
    # This should NOT raise an exception after the fix
    broker.push_commission_and_fees_report(mock_commission_report)
    
    # Assert - Verify order.execute was called with correct commission values
    order.execute.assert_called_once()
    call_args = order.execute.call_args[0]  # Get positional arguments
    
    # Extract commission values from order.execute call
    # Parameters: (dt, size, price, closed, closedvalue, closedcomm, opened, openedvalue, openedcomm, margin, pnl, psize, pprice)
    closedvalue = call_args[4]  # closedvalue should be 0.0 for None comminfo
    openedvalue = call_args[7]  # openedvalue should be 0.0 for None comminfo
    
    assert closedvalue == 0.0, f"Expected closedvalue=0.0 for None comminfo, got {closedvalue}"
    assert openedvalue == 0.0, f"Expected openedvalue=0.0 for None comminfo, got {openedvalue}"


def test_push_commissionreport_calculates_with_valid_comminfo(
    ib_broker_with_position_mock, mock_execution, mock_commission_report  
):
    """
    BUSINESS LOGIC TEST: Verify push_commissionreport() calculates costs correctly with valid comminfo
    and passes calculated commission values to order.execute()
    """
    # Arrange - Setup IBBroker internal state  
    broker = ib_broker_with_position_mock
    broker.executions[mock_execution.execId] = mock_execution
    
    # Create order with valid comminfo (normal scenario)
    order = Mock()
    mock_comminfo = Mock()
    mock_comminfo.getoperationcost.return_value = 15000.0  # 100 shares * $150
    order.comminfo = mock_comminfo
    order.data = Mock()
    order.data.close = [150.0]
    order.execute = Mock()
    
    broker.orderbyid[mock_execution.orderId] = order
    broker.ordstatus[mock_execution.orderId] = {mock_execution.cumQty: Mock()}
    
    # Act - Call the actual business method
    broker.push_commission_and_fees_report(mock_commission_report)
    
    # Assert - Verify order.execute was called with calculated commission values
    order.execute.assert_called_once()
    call_args = order.execute.call_args[0]  # Get positional arguments
    
    # Extract commission values from order.execute call
    # Parameters: (dt, size, price, closed, closedvalue, closedcomm, opened, openedvalue, openedcomm, margin, pnl, psize, pprice)
    closedvalue = call_args[4]  # closedvalue should be calculated by comminfo
    openedvalue = call_args[7]  # openedvalue should be calculated by comminfo
    
    assert closedvalue == 15000.0, f"Expected closedvalue=15000.0 from comminfo calculation, got {closedvalue}"
    assert openedvalue == 15000.0, f"Expected openedvalue=15000.0 from comminfo calculation, got {openedvalue}"
    
    # Verify comminfo.getoperationcost was called twice (for closed and opened positions)
    assert mock_comminfo.getoperationcost.call_count == 2
