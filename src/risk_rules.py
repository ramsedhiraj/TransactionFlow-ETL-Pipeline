"""
TransactionFlow Risk Monitoring Module
Configurable rule-based heuristic risk indicators for suspicious transaction patterns.

DISCLAIMER: These heuristic indicators are deterministic rule-based checks designed
for demonstration and operational alerting. They do NOT constitute an accredited or
regulatory-compliant fraud detection model.
"""

import json
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from src.models import RiskFlagRecord, RiskSeverityEnum, ValidatedTransaction

logger = logging.getLogger("transactionflow.risk_rules")


class BaseRiskRule:
    """Abstract base class for risk detection rules."""

    def __init__(self, rule_name: str, enabled: bool = True):
        self.rule_name = rule_name
        self.enabled = enabled

    def evaluate(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> List[RiskFlagRecord]:
        raise NotImplementedError


class HighAmountRule(BaseRiskRule):
    """
    Flags transactions that exceed a configured amount threshold.
    """

    def __init__(
        self,
        threshold: float = 10000.00,
        critical_threshold: float = 25000.00,
        enabled: bool = True,
    ):
        super().__init__(rule_name="HIGH_TRANSACTION_AMOUNT", enabled=enabled)
        self.threshold = Decimal(str(threshold))
        self.critical_threshold = Decimal(str(critical_threshold))

    def evaluate(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> List[RiskFlagRecord]:
        if not self.enabled:
            return []

        flags: List[RiskFlagRecord] = []
        for rec in records:
            if rec.transaction_amount >= self.threshold:
                severity = (
                    RiskSeverityEnum.CRITICAL.value
                    if rec.transaction_amount >= self.critical_threshold
                    else RiskSeverityEnum.HIGH.value
                )
                flags.append(
                    RiskFlagRecord(
                        pipeline_run_id=pipeline_run_id,
                        transaction_id=rec.transaction_id,
                        rule_name=self.rule_name,
                        severity=severity,
                        details=json.dumps(
                            {
                                "amount": str(rec.transaction_amount),
                                "currency": rec.currency,
                                "threshold": str(self.threshold),
                                "customer_id": rec.customer_id,
                            }
                        ),
                    )
                )
        return flags


class CustomerVelocityRule(BaseRiskRule):
    """
    Flags customers with an abnormally high transaction frequency within a time window.
    Example: > 3 transactions within 5 minutes.
    """

    def __init__(
        self,
        window_minutes: int = 5,
        max_allowed_count: int = 3,
        enabled: bool = True,
    ):
        super().__init__(rule_name="HIGH_FREQUENCY_VELOCITY", enabled=enabled)
        self.window_delta = timedelta(minutes=window_minutes)
        self.max_allowed_count = max_allowed_count
        # Sliding history: customer_id -> deque of (timestamp, transaction_id)
        self.history: Dict[str, deque] = defaultdict(deque)

    def evaluate(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> List[RiskFlagRecord]:
        if not self.enabled:
            return []

        flags: List[RiskFlagRecord] = []
        for rec in records:
            cust_queue = self.history[rec.customer_id]
            curr_ts = rec.transaction_timestamp

            # Purge older transactions outside the window
            while cust_queue and (curr_ts - cust_queue[0][0]) > self.window_delta:
                cust_queue.popleft()

            cust_queue.append((curr_ts, rec.transaction_id))

            if len(cust_queue) > self.max_allowed_count:
                flags.append(
                    RiskFlagRecord(
                        pipeline_run_id=pipeline_run_id,
                        transaction_id=rec.transaction_id,
                        rule_name=self.rule_name,
                        severity=RiskSeverityEnum.MEDIUM.value,
                        details=json.dumps(
                            {
                                "customer_id": rec.customer_id,
                                "tx_count_in_window": len(cust_queue),
                                "window_minutes": int(self.window_delta.total_seconds() / 60),
                                "prior_tx_ids": [t[1] for t in list(cust_queue)[:-1]],
                            }
                        ),
                    )
                )
        return flags

    def clear(self):
        self.history.clear()


class RapidIdenticalTransactionRule(BaseRiskRule):
    """
    Flags duplicate amounts for the same customer within a short interval (e.g., <= 60 seconds).
    Often indicates retry loops, double charges, or card testing.
    """

    def __init__(
        self,
        window_seconds: int = 60,
        enabled: bool = True,
    ):
        super().__init__(rule_name="RAPID_IDENTICAL_TRANSACTION", enabled=enabled)
        self.window_delta = timedelta(seconds=window_seconds)
        # customer_id -> list of (timestamp, amount, transaction_id)
        self.history: Dict[str, List[Tuple[datetime, Decimal, str]]] = defaultdict(list)

    def evaluate(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> List[RiskFlagRecord]:
        if not self.enabled:
            return []

        flags: List[RiskFlagRecord] = []
        for rec in records:
            prior_list = self.history[rec.customer_id]
            curr_ts = rec.transaction_timestamp

            # Filter old events
            recent = [item for item in prior_list if (curr_ts - item[0]) <= self.window_delta]
            self.history[rec.customer_id] = recent

            # Check for identical amount
            matched = [item for item in recent if item[1] == rec.transaction_amount and item[2] != rec.transaction_id]

            if matched:
                flags.append(
                    RiskFlagRecord(
                        pipeline_run_id=pipeline_run_id,
                        transaction_id=rec.transaction_id,
                        rule_name=self.rule_name,
                        severity=RiskSeverityEnum.HIGH.value,
                        details=json.dumps(
                            {
                                "customer_id": rec.customer_id,
                                "amount": str(rec.transaction_amount),
                                "window_seconds": int(self.window_delta.total_seconds()),
                                "matching_tx_id": matched[0][2],
                            }
                        ),
                    )
                )

            recent.append((curr_ts, rec.transaction_amount, rec.transaction_id))

        return flags

    def clear(self):
        self.history.clear()


class AtmWithdrawalThresholdRule(BaseRiskRule):
    """
    Flags ATM withdrawals or cash out events exceeding daily ATM threshold.
    """

    def __init__(
        self,
        threshold: float = 2000.00,
        enabled: bool = True,
    ):
        super().__init__(rule_name="ATM_WITHDRAWAL_THRESHOLD", enabled=enabled)
        self.threshold = Decimal(str(threshold))

    def evaluate(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> List[RiskFlagRecord]:
        if not self.enabled:
            return []

        flags: List[RiskFlagRecord] = []
        for rec in records:
            is_atm = rec.transaction_type == "WITHDRAWAL" or (
                rec.merchant_category and rec.merchant_category.upper() == "ATM"
            )
            if is_atm and rec.transaction_amount >= self.threshold:
                flags.append(
                    RiskFlagRecord(
                        pipeline_run_id=pipeline_run_id,
                        transaction_id=rec.transaction_id,
                        rule_name=self.rule_name,
                        severity=RiskSeverityEnum.MEDIUM.value,
                        details=json.dumps(
                            {
                                "customer_id": rec.customer_id,
                                "amount": str(rec.transaction_amount),
                                "threshold": str(self.threshold),
                                "transaction_type": rec.transaction_type,
                            }
                        ),
                    )
                )
        return flags


class RiskEngine:
    """
    Coordinates and evaluates all registered risk rules over batches of transactions.
    """

    def __init__(self, rules: Optional[List[BaseRiskRule]] = None):
        self.rules: List[BaseRiskRule] = rules or [
            HighAmountRule(),
            CustomerVelocityRule(),
            RapidIdenticalTransactionRule(),
            AtmWithdrawalThresholdRule(),
        ]

    def flag_suspicious_transactions(
        self,
        records: List[ValidatedTransaction],
        pipeline_run_id: str,
    ) -> List[RiskFlagRecord]:
        """
        Evaluate all active risk rules and collect flagged indicators.
        """
        all_flags: List[RiskFlagRecord] = []
        for rule in self.rules:
            flags = rule.evaluate(records, pipeline_run_id)
            all_flags.extend(flags)
        return all_flags

    def clear(self):
        """Reset stateful rules."""
        for rule in self.rules:
            if hasattr(rule, "clear"):
                rule.clear()
