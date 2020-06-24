import time

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata

from src.lib.configuration_objects import CodingModes


class ConsentUtils(object):
    @staticmethod
    def td_has_stop_code(td, coding_plans):
        """
        Returns whether any of the values for the given keys are Codes.STOP in the given TracedData object.

        :param td: TracedData object to search for stop codes.
        :type td: TracedData
        :param coding_plans:
        :type coding_plans: iterable of CodingPlan
        :return: Whether td contains Codes.STOP in any of the keys in 'keys'.
        :rtype: bool
        """
        for plan in coding_plans:
            for cc in plan.coding_configurations:
                if cc.coding_mode == CodingModes.SINGLE:
                    if cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"]).control_code == Codes.STOP:
                        return True
                else:
                    for label in td[cc.coded_field]:
                        if cc.code_scheme.get_code_with_code_id(label["CodeID"]).control_code == Codes.STOP:
                            return True
        return False

    @classmethod
    def determine_consent_withdrawn(cls, user, data, coding_plans, withdrawn_key="consent_withdrawn"):
        """
        Determines whether consent has been withdrawn, by searching for Codes.STOP in the given list of coding plans.

        TracedData objects where a stop code is found will have the key-value pair <withdrawn_key>: Codes.TRUE
        appended, or Codes.FALSE if no stop code is found.

        Note that this does not actually set any other keys to Codes.STOP. Use Consent.set_stopped for this purpose.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to determine consent for.
        :type data: iterable of TracedData
        :param coding_plans: Coding plans for the fields to search for stop codes.
        :type coding_plans: iterable of CodingPlan
        :param withdrawn_key: Name of key to use for the consent withdrawn field.
        :type withdrawn_key: str
        """
        for td in data:
            td.append_data({withdrawn_key: Codes.FALSE}, Metadata(user, Metadata.get_call_location(), time.time()))

        stopped_uids = set()
        for td in data:
            if cls.td_has_stop_code(td, coding_plans):
                stopped_uids.add(td["uid"])

        for td in data:
            if td["uid"] in stopped_uids:
                td.append_data(
                    {withdrawn_key: Codes.TRUE},
                    Metadata(user, Metadata.get_call_location(), time.time())
                )

    @staticmethod
    def set_stopped(user, data, withdrawn_key="consent_withdrawn", additional_keys=None):
        """
        For each TracedData object in an iterable whose 'withdrawn_key' is Codes.True, sets every other key to
        Codes.STOP. If there is no withdrawn_key or the value is not Codes.True, that TracedData object is not modified.

        :param user: Identifier of the user running this program, for TracedData Metadata.
        :type user: str
        :param data: TracedData objects to set to stopped if consent has been withdrawn.
        :type data: iterable of TracedData
        :param withdrawn_key: Key in each TracedData object which indicates whether consent has been withdrawn.
        :type withdrawn_key: str
        :param additional_keys: Additional keys to set to 'STOP' (e.g. keys not already in some TracedData objects)
        :type additional_keys: list of str | None
        """
        if additional_keys is None:
            additional_keys = []

        for td in data:
            if td.get(withdrawn_key) == Codes.TRUE:
                stop_dict = {key: Codes.STOP for key in list(td.keys()) + additional_keys if key != withdrawn_key}
                td.append_data(stop_dict, Metadata(user, Metadata.get_call_location(), time.time()))
