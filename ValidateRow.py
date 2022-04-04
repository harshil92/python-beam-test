import re
from typing import Any

import apache_beam as beam
from apache_beam.metrics import Metrics


class ValidateRow(beam.DoFn):
    """The function checks the following:
        - if the row contains 4 columns
        - if the timestamp and transaction_amount are valid.
        On failure, it will increment a counter and discard that row."""

    def __init__(self):
        self.failing_row_count = Metrics.counter('main', 'failing_row_count')
        self.failing_timestamp_count = Metrics.counter('main', 'failing_timestamp_count')
        self.failing_transaction_amount_count = Metrics.counter('main', 'failing_transaction_amount_count')

    def process(self, element):
        row = element.split(",")
        if len(row) != 4:
            self.failing_row_count.inc()
            return None
        elif not re.match('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', row[0][0:19]):
            self.failing_timestamp_count.inc()
            return None
        elif not is_float(row[3]):
            self.failing_transaction_amount_count.inc()
            return None
        else:
            yield element


def is_float(element: Any) -> bool:
    try:
        float(element)
        return True
    except ValueError:
        return False
