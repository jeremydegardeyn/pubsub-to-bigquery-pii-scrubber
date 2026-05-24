import argparse
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from pipeline.schema import BQ_SCHEMA, DEAD_LETTER_SCHEMA
from pipeline.transforms import ParsePubSubMessage, ScrubPIIDoFn


def run(argv=None):
    parser = argparse.ArgumentParser(description="PII-scrubbing Pub/Sub → BigQuery pipeline")
    parser.add_argument(
        "--input_topic",
        required=True,
        help="Full Pub/Sub topic path: projects/<project>/topics/<topic>",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery destination table: <project>:<dataset>.<table>",
    )
    parser.add_argument(
        "--dead_letter_table",
        default=None,
        help="BigQuery dead-letter table for unparseable messages (optional)",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadFromPubSub" >> ReadFromPubSub(
                topic=known_args.input_topic,
                with_attributes=True,
            )
            | "ParseMessages" >> beam.ParDo(ParsePubSubMessage()).with_outputs(
                "dead_letter", main="valid"
            )
        )

        _ = (
            parsed.valid
            | "ScrubPII" >> beam.ParDo(ScrubPIIDoFn())
            | "WriteToBigQuery" >> WriteToBigQuery(
                known_args.output_table,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                insert_retry_strategy="RETRY_ON_TRANSIENT_ERROR",
            )
        )

        if known_args.dead_letter_table:
            _ = (
                parsed.dead_letter
                | "WriteDeadLetter" >> WriteToBigQuery(
                    known_args.dead_letter_table,
                    schema=DEAD_LETTER_SCHEMA,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
