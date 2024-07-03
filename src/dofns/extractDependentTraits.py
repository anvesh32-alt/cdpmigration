import logging
import json

import apache_beam as beam


class ExtractDependentTraits(beam.DoFn):
    """
    Extension of beam DoFn to extract dependents from dependent object.
    """

    def process(self, element):
        """
        Flattens the nested json structure of the dependent object and extract them as individual traits
        in the 'traits' payload, can handle any level of nested structure.
        :param element: pcollection
        :return: pcollection element post pulling out dependents to send to next step in pipeline
        """

        data = json.loads(element)

        try:
            if data['type'] == 'identify' and data['traits'] != None and 'traits' in data and 'dependents' in data['traits'] and len(data['traits'])!= 0:
                base_prefix = 'dependent'

                # create a stack as a tuple with format (data, prefix)
                stack = [(data['traits']['dependents'], base_prefix)]
                individual_traits = {}

                # Flatten the nested dictionary until the stack is empty
                while stack:
                    dependent_trait, prefix = stack.pop()

                    if isinstance(dependent_trait, dict):
                        for dependent_key, dependent_value in dependent_trait.items():
                            if dependent_value is not None:
                                new_key = f"{prefix}{dependent_key[0].title()}{dependent_key[1:]}"
                                stack.append((dependent_value, new_key))

                    elif isinstance(dependent_trait, list):
                        for index, value in enumerate(dependent_trait, start=1):
                            new_key = f"{prefix}{index}"
                            stack.append((value, new_key))
                    else:
                        individual_traits[prefix] = dependent_trait

                data['traits'].update(individual_traits)
                yield json.dumps(data, separators=(',', ':'), ensure_ascii=False)

            else:
                yield element
        except KeyError:
            yield element
        except Exception as err:
            logging.exception(f"Error While Processing Records, Error: {str(err)}")
            