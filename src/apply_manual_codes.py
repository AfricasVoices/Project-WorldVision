import time
from os import path

from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.traced_data.io import TracedDataCodaV2IO

from src.lib import PipelineConfiguration
from src.lib.configuration_objects import CodingModes

log = Logger(__name__)


class ApplyManualCodes(object):
    @staticmethod
    def _impute_coding_error_codes(user, data):
        for td in data:
            coding_error_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                rqa_codes = []
                for cc in plan.coding_configurations:
                    if cc.coding_mode == CodingModes.SINGLE:
                        if cc.coded_field in td:
                            label = td[cc.coded_field]
                            rqa_codes.append(cc.code_scheme.get_code_with_code_id(label["CodeID"]))
                    else:
                        assert cc.coding_mode == CodingModes.MULTIPLE
                        for label in td.get(cc.coded_field, []):
                            rqa_codes.append(cc.code_scheme.get_code_with_code_id(label["CodeID"]))

                has_ws_code_in_code_scheme = False
                for code in rqa_codes:
                    if code.control_code == Codes.WRONG_SCHEME:
                        has_ws_code_in_code_scheme = True

                has_ws_code_in_ws_scheme = False
                if f"{plan.raw_field}_correct_dataset" in td:
                    ws_code = PipelineConfiguration.WS_CORRECT_DATASET_SCHEME.get_code_with_code_id(
                        td[f"{plan.raw_field}_correct_dataset"]["CodeID"])
                    has_ws_code_in_ws_scheme = ws_code.code_type == "Normal" or ws_code.control_code == Codes.NOT_CODED

                if has_ws_code_in_code_scheme != has_ws_code_in_ws_scheme:
                    log.warning(f"Coding Error: {plan.raw_field}: {td[plan.raw_field]}")
                    coding_error_dict[f"{plan.raw_field}_correct_dataset"] = \
                        CleaningUtils.make_label_from_cleaner_code(
                            PipelineConfiguration.WS_CORRECT_DATASET_SCHEME,
                            PipelineConfiguration.WS_CORRECT_DATASET_SCHEME.get_code_with_control_code(Codes.CODING_ERROR),
                            Metadata.get_call_location(),
                        ).to_dict()

                    for cc in plan.coding_configurations:
                        if cc.coding_mode == CodingModes.SINGLE:
                            coding_error_dict[cc.coded_field] = \
                                CleaningUtils.make_label_from_cleaner_code(
                                    cc.code_scheme,
                                    cc.code_scheme.get_code_with_control_code(Codes.CODING_ERROR),
                                    Metadata.get_call_location()
                                ).to_dict()
                        else:
                            assert cc.coding_mode == CodingModes.MULTIPLE
                            coding_error_dict[cc.coded_field] = [
                                CleaningUtils.make_label_from_cleaner_code(
                                    cc.code_scheme,
                                    cc.code_scheme.get_code_with_control_code(Codes.CODING_ERROR),
                                    Metadata.get_call_location()
                                ).to_dict()
                            ]

            td.append_data(coding_error_dict, Metadata(user, Metadata.get_call_location(), time.time()))

    @classmethod
    def apply_manual_codes(cls, user, data, coda_input_dir):
        # Merge manually coded data into the cleaned dataset
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.coda_filename is None:
                continue

            coda_input_path = path.join(coda_input_dir, plan.coda_filename)

            for cc in plan.coding_configurations:
                f = None
                try:
                    if path.exists(coda_input_path):
                        f = open(coda_input_path, "r")

                    if cc.coding_mode == CodingModes.SINGLE:
                        TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                            user, data, plan.id_field, {cc.coded_field: cc.code_scheme}, f)
                    else:
                        TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable_multi_coded(
                            user, data, plan.id_field, {cc.coded_field: cc.code_scheme}, f)
                finally:
                    if f is not None:
                        f.close()

            f = None
            try:
                if path.exists(coda_input_path):
                    f = open(coda_input_path, "r")

                TracedDataCodaV2IO.import_coda_2_to_traced_data_iterable(
                    user, data, plan.id_field,
                    {f"{plan.raw_field}_correct_dataset": PipelineConfiguration.WS_CORRECT_DATASET_SCHEME}, f
                )
            finally:
                if f is not None:
                    f.close()

        # Label data for which there is no response as TRUE_MISSING.
        # Label data for which the response is the empty string as NOT_CODED.
        for td in data:
            missing_dict = dict()
            for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
                if plan.raw_field not in td:
                    for cc in plan.coding_configurations:
                        na_label = CleaningUtils.make_label_from_cleaner_code(
                            cc.code_scheme, cc.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                            Metadata.get_call_location()
                        ).to_dict()
                        missing_dict[cc.coded_field] = na_label if cc.coding_mode == CodingModes.SINGLE else [na_label]
                elif td[plan.raw_field] == "":
                    for cc in plan.coding_configurations:
                        nc_label = CleaningUtils.make_label_from_cleaner_code(
                            cc.code_scheme, cc.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                            Metadata.get_call_location()
                        ).to_dict()
                        missing_dict[cc.coded_field] = nc_label if cc.coding_mode == CodingModes.SINGLE else [nc_label]
            td.append_data(missing_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Mark data that is noise as Codes.NOT_CODED
        for td in data:
            if td.get("noise", False):
                nc_dict = dict()
                for plan in PipelineConfiguration.RQA_CODING_PLANS:
                    for cc in plan.coding_configurations:
                        if cc.coded_field not in td:
                            nc_label = CleaningUtils.make_label_from_cleaner_code(
                                cc.code_scheme, cc.code_scheme.get_code_with_control_code(Codes.NOT_CODED),
                                Metadata.get_call_location()
                            ).to_dict()
                            nc_dict[cc.coded_field] = nc_label if cc.coding_mode == CodingModes.SINGLE else [nc_label]
                td.append_data(nc_dict, Metadata(user, Metadata.get_call_location(), time.time()))

        # Run code imputation functions
        for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.code_imputation_function is not None:
                plan.code_imputation_function(user, data, plan.coding_configurations)

        cls._impute_coding_error_codes(user, data)

        return data
