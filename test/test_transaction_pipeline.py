import gzip
import logging
import os
import unittest

from apache_beam.io import ReadFromText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from main import run, ComputeTransactions

E2E_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-data.csv')
INVALID_ROW_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-row.csv')
INVALID_TIMESTAMP_TEST_DATA_PATH = os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-timestamp.csv')
TEST_RESULT_PATH = os.path.join(os.getcwd(), 'output/test_result')


class TestTransactionPipeline(unittest.TestCase):

    def test_e2e_pipeline(self):
        expected_result = ['date, total_amount\n', '2017-03-18, 2102.22\n', '2017-08-31, 13700000023.08\n',
                           '2018-02-27, 129.12\n']

        run(['--input=%s' % E2E_TEST_DATA_PATH,
             '--output=%s' % TEST_RESULT_PATH],
            save_main_session=False)

        with gzip.open(os.path.join(os.getcwd(), 'output/test_result.csv.gz'), 'r') as f:
            lines = f.readlines()
            u_lines = listDecode(lines)

        self.assertEqual(u_lines, expected_result)

    def test_e2e_pipeline_invalid_row(self):
        expected_result = ['date, total_amount\n', '2017-03-18, 2102.22\n',
                           '2018-02-27, 129.12\n']

        run(['--input=%s' % INVALID_ROW_TEST_DATA_PATH,
             '--output=%s' % TEST_RESULT_PATH],
            save_main_session=False)

        with gzip.open(os.path.join(os.getcwd(), 'output/test_result.csv.gz'), 'r') as f:
            lines = f.readlines()
            u_lines = listDecode(lines)

        self.assertEqual(u_lines, expected_result)

    def test_compute_transactions(self):
        expected_result = [('2017-03-18', 2102.22), ('2017-08-31', 13700000023.08), ('2018-02-27', 129.12)]

        with TestPipeline() as p:
            input_lines = p | ReadFromText(E2E_TEST_DATA_PATH,
                                           skip_header_lines=True)
            output = input_lines | ComputeTransactions()

            print(output)
            assert_that(
                output,
                equal_to(expected_result))


def listDecode(lines):
    u_lines = []
    for line in lines:
        u_lines.append(line.decode('utf-8'))
    return u_lines


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
