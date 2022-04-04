import logging
import os
import unittest

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from ValidateRow import ValidateRow

E2E_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-data.csv')
INVALID_ROW_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-row.csv')
INVALID_TIMESTAMP_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-timestamp.csv')
INVALID_TAMOUNT_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-transaction_amount.csv')


class TestValidateRowFunction(unittest.TestCase):

    def test_validate_row(self):
        expected_result = ['2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                           '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                           '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                           '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                           '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12']

        with TestPipeline() as p:
            input_lines = p | ReadFromText(INVALID_ROW_TEST_DATA_PATH,
                                           skip_header_lines=True)
            output = input_lines | beam.ParDo(ValidateRow())

            print(output)
            assert_that(
                output,
                equal_to(expected_result))

    def test_validate_row_invalid_timestamp(self):
        expected_result = ['2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                           '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                           '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                           '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12']

        with TestPipeline() as p:
            input_lines = p | ReadFromText(INVALID_TIMESTAMP_TEST_DATA_PATH,
                                           skip_header_lines=True)
            output = input_lines | beam.ParDo(ValidateRow())

            print(output)
            assert_that(
                output,
                equal_to(expected_result))


    def test_validate_row_invalid_transaction_amount(self):
        expected_result = ['2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                           '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                           '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                           '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12']

        with TestPipeline() as p:
            input_lines = p | ReadFromText(INVALID_TAMOUNT_TEST_DATA_PATH,
                                           skip_header_lines=True)
            output = input_lines | beam.ParDo(ValidateRow())

            print(output)
            assert_that(
                output,
                equal_to(expected_result))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
