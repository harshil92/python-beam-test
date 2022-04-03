import gzip
import logging
import os
import unittest

import apache_beam as beam
import pytest
from apache_beam.io import ReadFromText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from main import run, ComputeTransactions, ValidateRow


@pytest.mark.examples_postcommit
class TestTransactionPipeline(unittest.TestCase):

    def test_e2e_pipeline(self):
        expected_result = [b'date, total_amount\n', b'2017-03-18, 2102.22\n', b'2017-08-31, 13700000023.08\n',
                           b'2018-02-27, 129.12\n']

        run(['--input=%s' % os.path.join(os.getcwd(), 'test/resources/e2e-test-data.csv'),
             '--output=%s' % os.path.join(os.getcwd(), 'output/test_result')],
            save_main_session=False)

        with gzip.open(os.path.join(os.getcwd(), 'output/test_result.csv.gz'), 'r') as f:
            lines = f.readlines()

        self.assertEqual(lines, expected_result)

    def test_e2e_pipeline_invalid_row(self):
        expected_result = [b'date, total_amount\n', b'2017-03-18, 2102.22\n',
                           b'2018-02-27, 129.12\n']

        run(['--input=%s' % os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-row.csv'),
             '--output=%s' % os.path.join(os.getcwd(), 'output/test_result')],
            save_main_session=False)

        with gzip.open(os.path.join(os.getcwd(), 'output/test_result.csv.gz'), 'r') as f:
            lines = f.readlines()

        self.assertEqual(lines, expected_result)

    def test_compute_transactions(self):
        expected_result = [('2017-03-18', 2102.22), ('2017-08-31', 13700000023.08), ('2018-02-27', 129.12)]

        with TestPipeline() as p:
            input = p | ReadFromText(os.path.join(os.getcwd(), 'test/resources/e2e-test-data.csv'),
                                     skip_header_lines=True)
            output = input | ComputeTransactions()

            print(output)
            assert_that(
                output,
                equal_to(expected_result))

    def test_validate_row(self):
        expected_result = ['2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99',
                           '2017-01-01 04:22:23 UTC,wallet00000e719adfeaa64b5a,wallet00001e494c12b3083634,19.95',
                           '2017-03-18 14:09:16 UTC,wallet00001866cb7e0f09a890,wallet00001e494c12b3083634,2102.22',
                           '2017-03-18 14:10:44 UTC,wallet00001866cb7e0f09a890,wallet00000e719adfeaa64b5a,1.00030',
                           '2018-02-27 16:04:11 UTC,wallet00005f83196ec58e4ffe,wallet00001866cb7e0f09a890,129.12']

        with TestPipeline() as p:
            input = p | ReadFromText(os.path.join(os.getcwd(), 'test/resources/e2e-test-invalid-row.csv'),
                                     skip_header_lines=True)
            output = input | beam.ParDo(ValidateRow())

            print(output)
            assert_that(
                output,
                equal_to(expected_result))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
