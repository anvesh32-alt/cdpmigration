from apache_beam.options.pipeline_options import PipelineOptions


class UserPipelineOptions(PipelineOptions):
    """
    Extension of beam PipelineOptions where optional user inputs are added
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        """
        class method adding custom parameters
        :param parser: main argument parser, used to store values of command line/composer inputs
        """

        parser.add_argument(
            "--input_bucket",
            dest="input_bucket",
            required=True,
            help="The GCS bucket to read data from.",
        )

        parser.add_argument(
            "--output_bucket",
            dest="output_bucket",
            required=True,
            help="The GCS bucket to write data to."
        )

        parser.add_argument(
            "--mpn",
            dest="mpn",
            required=True,
            help="Mpn(Marketing program number) for which pipeline should run."
        )

        parser.add_argument(
            "--pipeline_name",
            dest="pipeline_name",
            required=False,
            default='migration_pipeline',
            help="Pipeline name for which pipeline should run."
        )
