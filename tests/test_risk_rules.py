"""
Unit tests for heuristic transaction risk monitoring rules.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal

from src.models import RiskSeverityEnum, ValidatedTransaction
from src.risk_rules import (
    AtmWithdrawalThresholdRule,
    CustomerVelocityRule,
    HighAmountRule,
    RapidIdenticalTransactionRule,
    RiskEngine,
)


def make_test_record(
    tx_id: str,
    cust_id: str,
    amount: float,
    ts: datetime,
    tx_type: str = "PURCHASE",
    category: str = "RETAIL",
) -> ValidatedTransaction:
    return ValidatedTransaction(
        transaction_id=tx_id,
        source_record_id=f"SRC-{tx_id}",
        source_system="TEST_FEED",
        customer_id=cust_id,
        transaction_timestamp=ts,
        transaction_amount=Decimal(f"{amount:.2f}"),
        currency="USD",
        transaction_type=tx_type,
        transaction_status="COMPLETED",
        merchant_category=category,
        pipeline_run_id="RUN-RISK-TEST",
    )


def test_high_amount_rule():
    rule = HighAmountRule(threshold=10000.00, critical_threshold=25000.00)
    base_ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    rec_normal = make_test_record("TXN-1", "CUST-1", 500.00, base_ts)
    rec_high = make_test_record("TXN-2", "CUST-2", 15000.00, base_ts)
    rec_critical = make_test_record("TXN-3", "CUST-3", 30000.00, base_ts)

    flags = rule.evaluate([rec_normal, rec_high, rec_critical], pipeline_run_id="RUN-01")

    assert len(flags) == 2
    assert flags[0].transaction_id == "TXN-2"
    assert flags[0].severity == RiskSeverityEnum.HIGH.value
    assert flags[1].transaction_id == "TXN-3"
    assert flags[1].severity == RiskSeverityEnum.CRITICAL.value


def test_customer_velocity_rule():
    rule = CustomerVelocityRule(window_minutes=5, max_allowed_count=2)
    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 12, 2, 0, tzinfo=timezone.utc)
    t3_outside = datetime(2026, 1, 1, 12, 10, 0, tzinfo=timezone.utc)  # outside 5-min window

    recs = [
        make_test_record("TXN-V1", "CUST-BURST", 50.00, t0),
        make_test_record("TXN-V2", "CUST-BURST", 60.00, t1),
        make_test_record("TXN-V3", "CUST-BURST", 70.00, t2),  # 3rd tx in 2 mins -> triggers rule
        make_test_record("TXN-V4", "CUST-BURST", 80.00, t3_outside),  # after window reset
    ]

    flags = rule.evaluate(recs, pipeline_run_id="RUN-01")
    assert len(flags) == 1
    assert flags[0].transaction_id == "TXN-V3"
    assert flags[0].rule_name == "HIGH_FREQUENCY_VELOCITY"


def test_rapid_identical_transaction_rule():
    rule = RapidIdenticalTransactionRule(window_seconds=60)
    t0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 1, 1, 12, 0, 30, tzinfo=timezone.utc)  # 30 seconds later, same amount

    recs = [
        make_test_record("TXN-R1", "CUST-REPEAT", 199.99, t0),
        make_test_record("TXN-R2", "CUST-REPEAT", 199.99, t1),
    ]

    flags = rule.evaluate(recs, pipeline_run_id="RUN-01")
    assert len(flags) == 1
    assert flags[0].transaction_id == "TXN-R2"
    assert flags[0].rule_name == "RAPID_IDENTICAL_TRANSACTION"

    details = json.loads(flags[0].details)
    assert details["amount"] == "199.99"
    assert details["matching_tx_id"] == "TXN-R1"


def test_atm_withdrawal_rule():
    rule = AtmWithdrawalThresholdRule(threshold=2000.00)
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    normal_atm = make_test_record("TXN-A1", "CUST-1", 400.00, ts, tx_type="WITHDRAWAL", category="ATM")
    excessive_atm = make_test_record("TXN-A2", "CUST-2", 2500.00, ts, tx_type="WITHDRAWAL", category="ATM")
    retail_purchase = make_test_record("TXN-A3", "CUST-3", 3000.00, ts, tx_type="PURCHASE", category="RETAIL")

    flags = rule.evaluate([normal_atm, excessive_atm, retail_purchase], pipeline_run_id="RUN-01")
    assert len(flags) == 1
    assert flags[0].transaction_id == "TXN-A2"
    assert flags[0].rule_name == "ATM_WITHDRAWAL_THRESHOLD"


def test_risk_engine_coordination():
    engine = RiskEngine()
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Clean transaction
    rec_clean = make_test_record("TXN-C1", "CUST-10", 45.00, ts)
    # High amount transaction
    rec_high = make_test_record("TXN-H1", "CUST-11", 12000.00, ts)

    flags = engine.flag_suspicious_transactions([rec_clean, rec_high], pipeline_run_id="RUN-TEST")
    assert len(flags) == 1
    assert flags[0].transaction_id == "TXN-H1"
