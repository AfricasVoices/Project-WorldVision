import random

from core_data_modules.logging import Logger

log = Logger(__name__)


# TODO: Move to Core
class ICRTools(object):
    @staticmethod
    def generate_sample_for_icr(data, sample_size, random_generator=None):
        # FIXME: Should data be de-duplicated before exporting for ICR?

        if random_generator is None:
            random_generator = random
        if len(data) < sample_size:
            log.warning(f"The size of the ICR data ({len(data)} items) is less than the requested sample_size "
                        f"({sample_size} items). Returning all the input data as ICR.")
            sample_size = len(data)

        return random_generator.sample(data, sample_size)
