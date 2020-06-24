from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils

log = Logger(__name__)


class LoadData(object):
    @staticmethod
    def load_datasets(raw_data_dir, flow_names):
        datasets = []
        for i, flow_name in enumerate(flow_names):
            raw_flow_path = f"{raw_data_dir}/{flow_name}.jsonl"
            log.info(f"Loading {i + 1}/{len(flow_names)}: {raw_flow_path}...")
            with open(raw_flow_path, "r") as f:
                runs = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
            log.info(f"Loaded {len(runs)} runs")
            datasets.append(runs)
        return datasets

    @staticmethod
    def coalesce_traced_runs_by_key(user, traced_runs, coalesce_key):
        coalesced_runs = dict()

        for run in traced_runs:
            if run[coalesce_key] not in coalesced_runs:
                coalesced_runs[run[coalesce_key]] = run
            else:
                coalesced_runs[run[coalesce_key]].append_data(
                    dict(run.items()), Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        return list(coalesced_runs.values())

    @staticmethod
    def combine_raw_datasets(user, messages_datasets, surveys_datasets):
        data = []

        for messages_dataset in messages_datasets:
            data.extend(messages_dataset)

        for surveys_dataset in surveys_datasets:
            TracedData.update_iterable(user, "avf_phone_id", data, surveys_dataset, "survey_responses")

        return data

    @classmethod
    def load_raw_data(cls, user, raw_data_dir, pipeline_configuration):
        activation_flow_names = []
        survey_flow_names = []
        for raw_data_source in pipeline_configuration.raw_data_sources:
            activation_flow_names.extend(raw_data_source.get_activation_flow_names())
            survey_flow_names.extend(raw_data_source.get_survey_flow_names())
            
        log.info("Loading activation datasets...")
        activation_datasets = cls.load_datasets(raw_data_dir, activation_flow_names)

        log.info("Loading survey datasets...")
        survey_datasets = cls.load_datasets(raw_data_dir, survey_flow_names)

        # Add survey data to the messages
        log.info("Combining Datasets...")
        coalesced_survey_datasets = []
        for dataset in survey_datasets:
            coalesced_survey_datasets.append(cls.coalesce_traced_runs_by_key(user, dataset, "avf_phone_id"))
        data = cls.combine_raw_datasets(user, activation_datasets, coalesced_survey_datasets)

        return data
