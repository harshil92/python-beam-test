import argparse
import datetime
import logging
import os
from decimal import Decimal

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from ValidateRow import ValidateRow


class ComputeTransactions(beam.PTransform):
    """This function parses the lines and applies the transforms."""

    def expand(self, pcoll):
        return pcoll \
               | 'validate row' >> beam.ParDo(ValidateRow()) \
               | 'Split' >> beam.Map(lambda line: line.split(",")) \
               | 'Schema' >> beam.Map(lambda field: beam.Row(
                                        timestamp=datetime.datetime.fromisoformat(field[0][0:19]),
                                        origin=field[1],
                                        destination=field[2],
                                        transaction_amount=Decimal(field[3]))) \
               | 'Filter amount > 20' >> beam.Filter(lambda row: row.transaction_amount > 20) \
               | 'Exclude year < 2010' >> beam.Filter(lambda row: row.timestamp.year > 2009) \
               | 'Reformat' >> beam.Map(lambda row: (str(row.timestamp.date()), row.transaction_amount)) \
               | 'Sum values per date' >> beam.CombinePerKey(sum)


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the pipeline."""
    result_path = os.path.join(os.getcwd(), 'output/result')

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default=result_path,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        lines = pipeline | 'Read' >> ReadFromText(known_args.input, skip_header_lines=True)
        date_total_amount = lines | ComputeTransactions()

        def csv_format_result(date, total_amount):
            return '%s, %s' % (date, total_amount)

        output = date_total_amount | 'Format' >> beam.MapTuple(csv_format_result)

        output | 'Write' >> WriteToText(known_args.output, num_shards=1, shard_name_template='',
                                        file_name_suffix=".csv.gz", compression_type=CompressionTypes.GZIP,
                                        header='date, total_amount')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
