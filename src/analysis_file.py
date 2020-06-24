from collections import OrderedDict

from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.traced_data.util import FoldTracedData
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import TimeUtils

from src.lib import PipelineConfiguration, ConsentUtils
from src.lib.configuration_objects import CodingModes


class AnalysisFile(object):
    @staticmethod
    def export_to_csv(user, data, csv_path, export_keys, consent_withdrawn_key):
        # Convert codes to their string/matrix values
        for td in data:
            analysis_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                for cc in plan.coding_configurations:
                    if cc.analysis_file_key is None:
                        continue

                    if cc.coding_mode == CodingModes.SINGLE:
                        analysis_dict[cc.analysis_file_key] = \
                            cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"]).string_value
                    else:
                        assert cc.coding_mode == CodingModes.MULTIPLE
                        show_matrix_keys = []
                        for code in cc.code_scheme.codes:
                            show_matrix_keys.append(f"{cc.analysis_file_key}{code.string_value}")

                        for label in td[cc.coded_field]:
                            code_string_value = cc.code_scheme.get_code_with_code_id(label["CodeID"]).string_value
                            analysis_dict[f"{cc.analysis_file_key}{code_string_value}"] = Codes.MATRIX_1

                        for key in show_matrix_keys:
                            if key not in analysis_dict:
                                analysis_dict[key] = Codes.MATRIX_0
            td.append_data(analysis_dict,
                           Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Hide data from participants who opted out
        ConsentUtils.set_stopped(user, data, consent_withdrawn_key, additional_keys=export_keys)

        with open(csv_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(data, f, headers=export_keys)

    @classmethod
    def generate(cls, user, data, csv_by_message_output_path, csv_by_individual_output_path):
        # Serializer is currently overflowing
        # TODO: Investigate/address the cause of this.
        # sys.setrecursionlimit(15000)

        # Set consent withdrawn based on presence of data coded as "stop"
        consent_withdrawn_key = "consent_withdrawn"
        ConsentUtils.determine_consent_withdrawn(
            user, data, PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS,
            consent_withdrawn_key
        )

        # Set the list of keys to be exported and how they are to be handled when folding
        fold_strategies = OrderedDict()
        fold_strategies["uid"] = FoldStrategies.assert_equal
        fold_strategies[consent_withdrawn_key] = FoldStrategies.boolean_or

        export_keys = ["uid", consent_withdrawn_key]

        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    export_keys.append(cc.analysis_file_key)
                else:
                    assert cc.coding_mode == CodingModes.MULTIPLE
                    for code in cc.code_scheme.codes:
                        export_keys.append(f"{cc.analysis_file_key}{code.string_value}")

                fold_strategies[cc.coded_field] = cc.fold_strategy

            export_keys.append(plan.raw_field)
            fold_strategies[plan.raw_field] = plan.raw_field_fold_strategy

        # Fold data to have one respondent per row
        to_be_folded = []
        for td in data:
            to_be_folded.append(td.copy())

        folded_data = FoldTracedData.fold_iterable_of_traced_data(
            user, to_be_folded, lambda td: td["uid"], fold_strategies
        )

        cls.export_to_csv(user, data, csv_by_message_output_path, export_keys, consent_withdrawn_key)
        cls.export_to_csv(user, folded_data, csv_by_individual_output_path, export_keys, consent_withdrawn_key)

        return data, folded_data
