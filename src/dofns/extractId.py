import apache_beam as beam
import json
import logging


class ExtractId(beam.DoFn):
    """
    Extension of beam DoFn filtering pcollection element to extract the userId,anonymousId for each record
    """

    def process(self, element):
        """
        Converts the input data to json and returns corresponding userId,anonymousId for each element
        :param element: pcollection of files read from gcs input bucket
        :return tuple of (userId or anonymousId , element): pcollection element to send to next step in pipeline
        """

        data = json.loads(element)

        try:
            if all(key in data for key in ("anonymousId", "userId")):
                yield data['userId'], element
            else:
                yield data['anonymousId'], element
        except KeyError:
            yield data['userId'], element
        except Exception as err:
            logging.error(f"Error While Reading Records, Error: {str(err)}")


class ExtractIdForGroupByOnEvents(beam.DoFn):
    """
    Extension of beam DoFn filtering pcollection element to extract the userId,events for clt events
    """

    def process(self, element, *args, **kwargs):
        """
        Converts the input data to json and returns corresponding userId,event for each element
        :param element: pcollection of clt events
        :return tuple of ((userId,event), element): pcollection element to send to next step in pipeline
        """

        data = json.loads(element)
        try:
            yield ((data['userId'], data['event']), data)
        except KeyError:
            pass
        except Exception as err:
            logging.exception(f"Error While Mapping Records to user ID, Error: {str(err)}")


class GetLatestRecordsForEachUser(beam.DoFn):
    """
    Extension of beam DoFn filtering pcollection element to find the latest event for each user
    """

    def process(self, element, *args, **kwargs):
        try:
            latest_record = max(element[1], key=lambda x: x['timestamp'])
            yield json.dumps(latest_record)
        except KeyError:
            pass
        except Exception as err:
            logging.error(f"Error While Extracting latest record for each user, Error: {str(err)}")
