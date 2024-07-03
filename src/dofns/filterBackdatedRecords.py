import datetime
import json
import logging

import apache_beam as beam


class FilterBackDatedRecords(beam.DoFn):
    """
    Extension of beam DoFn to Filter out records based on date for CLT,clickstream and other events
    """

    def __init__(self, current_date, filter_anonymous_id=False):
        """
        Constructs all the necessary attributes for the FilterBackdatedRecords object.
        :param current_date: current system date
        :param filter_anonymous_id: anonymous_id to be filtered or not by default the function will not
        """

        self.current_date = current_date
        self.filter_anonymous_id = filter_anonymous_id

    def setup(self):
        logging.info(f"Filtering records based on date: {self.current_date}, anonymous_id: {self.filter_anonymous_id}")

    def process(self, element, *args, **kwargs):
        """
        Filters out back dated records , 3 years from current date and 6 months for anonymous users
        :param element: pcollection of elements
        :return: pcollection after filtering based on originalTimestamp to next step in pipeline
        """

        data = json.loads(element)
        three_years_backdate = self.current_date - datetime.timedelta(days=365 * 3)
        six_months_backdate = self.current_date - datetime.timedelta(days=6 * 30)

        try:
            # check if records need to be filtered on anonymous_id
            if self.filter_anonymous_id:
                original_timestamp = datetime.datetime.strptime(data['originalTimestamp'],
                                                                "%Y-%m-%dT%H:%M:%S.%fZ").date()
                if 'anonymousId' in data:
                    # Pass only anonymous id records which are of past 6 months
                    if original_timestamp > six_months_backdate:
                        yield json.dumps(data)
                else:
                    # Pass only records which are of past 3 years
                    if original_timestamp > three_years_backdate:
                        yield json.dumps(data)

            else:
                if 'anonymousId' not in data:
                    original_timestamp = datetime.datetime.strptime(data['originalTimestamp'],
                                                                    "%Y-%m-%dT%H:%M:%S.%fZ").date()
                    # Pass only records which are of past 3 years
                    if original_timestamp > three_years_backdate:
                        yield json.dumps(data)

        except KeyError as err:
            logging.info(f"Key Not Found data: {data} err: {err} not found")

        except Exception as err:
            logging.exception(f"Error while filtering on timestamp err: {err}")
