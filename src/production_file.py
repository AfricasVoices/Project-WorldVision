from core_data_modules.traced_data.io import TracedDataCSVIO

from src.lib import PipelineConfiguration, MessageFilters


class ProductionFile(object):
    @staticmethod
    def generate(data, production_csv_output_path):
        production_keys = ["uid"]
        for plan in PipelineConfiguration.RQA_CODING_PLANS:
            if plan.raw_field not in production_keys:
                production_keys.append(plan.raw_field)
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            if plan.raw_field not in production_keys:
                production_keys.append(plan.raw_field)

        not_noise = MessageFilters.filter_noise(data, "noise", lambda x: x)
        with open(production_csv_output_path, "w") as f:
            TracedDataCSVIO.export_traced_data_iterable_to_csv(not_noise, f, headers=production_keys)

        return data
