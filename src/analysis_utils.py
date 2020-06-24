from core_data_modules.cleaners import Codes
from core_data_modules.data_models.code_scheme import CodeTypes

from src.lib.configuration_objects import CodingModes


class AnalysisUtils(object):
    @staticmethod
    def _get_td_codes_for_coding_configuration(td, cc):
        """
        Returns all the codes from a TracedData object applied under the given coding configuration.

        This is a helper method for many of the other methods in this class.
        
        :param td: TracedData to get the codes from.
        :type td: TracedData
        :param cc: Coding configuration.
        :type cc: src.lib.pipeline_configuration.CodingConfiguration
        :return: All the codes for the labels in the TracedData specified by the coding configuration `cc`.
        :rtype: list of Code
        """
        if cc.coding_mode == CodingModes.SINGLE:
            labels = [td[cc.coded_field]]
        else:
            assert cc.coding_mode == CodingModes.MULTIPLE
            labels = td[cc.coded_field]

        return [cc.code_scheme.get_code_with_code_id(label["CodeID"]) for label in labels]

    @classmethod
    def responded(cls, td, coding_plan):
        """
        Returns whether the given TracedData object contains a response to the given coding plan.

        A response is any field that hasn't been labelled with either TRUE_MISSING or SKIPPED.
        This includes participants who withdrew their consent to have their data analysed.

        :param td: TracedData to check. 
        :type td: TracedData
        :param coding_plan: A coding plan specifying the field names to look up in `td`, and the code scheme to use
                            to interpret those values.
        :type coding_plan: src.lib.pipeline_configuration.CodingPlan
        :return: Whether `td` contains a response to `coding_plan`.
        :rtype: bool
        """
        for cc in coding_plan.coding_configurations:
            codes = cls._get_td_codes_for_coding_configuration(td, cc)
            assert len(codes) >= 1
            if len(codes) > 1:
                # If there is an NA or NS code, there shouldn't be any other codes present.
                for code in codes:
                    assert code.control_code != Codes.TRUE_MISSING and code.control_code != Codes.SKIPPED
                return True
            return codes[0].control_code != Codes.TRUE_MISSING and codes[0].control_code != Codes.SKIPPED

    @staticmethod
    def withdrew_consent(td, consent_withdrawn_key):
        """
        Returns whether the given TracedData object represents someone who withdrew their consent to have their data 
        analysed.
        
        :param td: TracedData to check.
        :type td: TracedData
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :return: Whether consent was withdrawn.
        :rtype: bool
        """
        return td[consent_withdrawn_key] == Codes.TRUE
    
    @classmethod
    def opt_in(cls, td, consent_withdrawn_key, coding_plan):
        """
        Returns whether the given TracedData object contains a response to the given coding_plan.

        A response is any field that hasn't been labelled with either TRUE_MISSING or SKIPPED.
        Returns False for participants who withdrew their consent to have their data analysed.

        :param td: TracedData to check.
        :type td: TracedData
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plan: A coding plan specifying the field names to look up in `td`, and the code scheme to use
                            to interpret those values.
        :type coding_plan: src.lib.pipeline_configuration.CodingPlan
        :return: Whether `td` contains a response to `coding_plan` and did not withdraw consent.
        :rtype: bool
        """
        return not cls.withdrew_consent(td, consent_withdrawn_key) and cls.responded(td, coding_plan)

    @classmethod
    def labelled(cls, td, consent_withdrawn_key, coding_plan):
        """
        Returns whether the given TracedData object has been labelled under the given coding_plan.

        An object is considered labelled if all of the following hold:
         - Consent was not withdrawn.
         - A response was received (see `AnalysisUtils.responded` for the definition of this).
         - The response has been assigned at least one label under each coding configuration.
         - None of the assigned labels have the control code NOT_REVIEWED.

        :param td: TracedData to check.
        :type td: TracedData
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plan: A coding plan specifying the field names to look up in `td`, and the code scheme to use
                            to interpret those values.
        :type coding_plan: src.lib.pipeline_configuration.CodingPlan
        :return: Whether `td` contains a labelled response to `coding_plan` and did not withdraw consent.
        :rtype: bool
        """
        if cls.withdrew_consent(td, consent_withdrawn_key):
            return False
        
        if not cls.responded(td, coding_plan):
            return False

        for cc in coding_plan.coding_configurations:
            codes = cls._get_td_codes_for_coding_configuration(td, cc)
            if len(codes) == 0:
                return False
            for code in codes:
                if code.control_code == Codes.NOT_REVIEWED:
                    return False
        return True

    @classmethod
    def relevant(cls, td, consent_withdrawn_key, coding_plan):
        """
        Returns whether the given TracedData object contains a relevant response to the given coding_plan.

        A response is considered relevant if it is labelled with a normal code under any of its coding configurations.
        Returns False for participants who withdrew their consent to have their data analysed.

        :param td: TracedData to check.
        :type td: TracedData
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plan: A coding plan specifying the field names to look up in `td`, and the code scheme to use
                            to interpret those values.
        :type coding_plan: src.lib.pipeline_configuration.CodingPlan
        :return: Whether `td` contains a relevant response to `coding_plan`.
        :rtype: bool
        """
        if cls.withdrew_consent(td, consent_withdrawn_key):
            return False
        
        for cc in coding_plan.coding_configurations:
            codes = cls._get_td_codes_for_coding_configuration(td, cc)
            for code in codes:
                if code.code_type == CodeTypes.NORMAL:
                    return True
        return False
    
    @classmethod
    def filter_responded(cls, data, coding_plans):
        """
        Filters a list of message or participant data for objects that responded to at least one of the given coding
        plans.

        For the definition of "responded", see `AnalysisUtils.responded`

        :param data: Message or participant data to filter.
        :type data: TracedData iterable
        :param coding_plans: Coding plans specifying the fields in each TracedData object in `data` to look up.
        :type coding_plans: list of src.lib.pipeline_configuration.CodingPlan
        :return: data, filtered for only the objects that responded to at least one of the coding plans.
        :rtype: list of TracedData
        """
        responded = []
        for td in data:
            for plan in coding_plans:
                if cls.responded(td, plan):
                    responded.append(td)
                    break
        return responded

    @classmethod
    def filter_opt_ins(cls, data, consent_withdrawn_key, coding_plans):
        """
        Filters a list of message or participant data for objects that opted-in and contained a response to at least
        one of the given coding plans.

        For the definition of "opted-in", see `AnalysisUtils.opt_in`

        :param data: Message or participant data to filter.
        :type data: TracedData iterable
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plans: Coding plans specifying the fields in each TracedData object in `data` to look up.
        :type coding_plans: list of src.lib.pipeline_configuration.CodingPlan
        :return: data, filtered for only the objects that opted-in and responded to at least one of the coding plans.
        :rtype: list of TracedData
        """
        opt_ins = []
        for td in data:
            for plan in coding_plans:
                if cls.opt_in(td, consent_withdrawn_key, plan):
                    opt_ins.append(td)
                    break
        return opt_ins

    @classmethod
    def filter_partially_labelled(cls, data, consent_withdrawn_key, coding_plans):
        """
        Filters a list of message or participant data for objects that opted-in and are fully labelled under at least
        one of the given coding plans.

        For the definition of "labelled", see `AnalysisUtils.labelled`

        :param data: Message or participant data to filter.
        :type data: TracedData iterable
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plans: Coding plans specifying the fields in each TracedData object in `data` to look up.
        :type coding_plans: list of src.lib.pipeline_configuration.CodingPlan
        :return: `data`, filtered for only the objects that opted-in and are labelled under at least one of the coding
                 plans.
        :rtype: list of TracedData
        """
        labelled = []
        for td in data:
            for plan in coding_plans:
                if cls.labelled(td, consent_withdrawn_key, plan):
                    labelled.append(td)
                    break
        return labelled

    @classmethod
    def filter_fully_labelled(cls, data, consent_withdrawn_key, coding_plans):
        """
        Filters a list of message or participant data for objects that opted-in and are fully labelled under all of
        the given coding plans.

        For the definition of "labelled", see `AnalysisUtils.labelled`

        :param data: Message or participant data to filter.
        :type data: TracedData iterable
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plans: Coding plans specifying the fields in each TracedData object in `data` to look up.
        :type coding_plans: list of src.lib.pipeline_configuration.CodingPlan
        :return: data, filtered for only the objects that opted-in and are labelled under all of the coding plans.
        :rtype: list of TracedData
        """
        labelled = []
        for td in data:
            td_is_labelled = True
            for plan in coding_plans:
                if not cls.labelled(td, consent_withdrawn_key, plan):
                    td_is_labelled = False

            if td_is_labelled:
                labelled.append(td)

        return labelled

    @classmethod
    def filter_relevant(cls, data, consent_withdrawn_key, coding_plans):
        """
        Filters a list of message or participant data for objects that are relevant to at least one of the given coding
        plans.

        For the definition of "relevant", see `AnalysisUtils.relevant`

        :param data: Message or participant data to filter.
        :type data: TracedData iterable
        :param consent_withdrawn_key: Key in the TracedData of the consent withdrawn field.
        :type consent_withdrawn_key: str
        :param coding_plans: Coding plans specifying the fields in each TracedData object in `data` to look up.
        :type coding_plans: list of src.lib.pipeline_configuration.CodingPlan
        :return: data, filtered for only the objects that are relevant to at least one of the coding plans.
        :rtype: list of TracedData
        """
        relevant = []
        for td in data:
            for plan in coding_plans:
                if cls.relevant(td, consent_withdrawn_key, plan):
                    relevant.append(td)
                    break
        return relevant
