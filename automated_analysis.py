import argparse
import csv
import json
import random
from collections import OrderedDict
from glob import glob

import geopandas
import matplotlib.pyplot as plt
import plotly.express as px
from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.codes import KenyaCodes
from core_data_modules.cleaners.location_tools import KenyaLocations
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import IOUtils
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from configuration.code_schemes import CodeSchemes
from src import AnalysisUtils
from src.lib import PipelineConfiguration
from src.lib.configuration_objects import CodingModes
from src.mapping_utils import MappingUtils

log = Logger(__name__)

IMG_SCALE_FACTOR = 10  # Increase this to increase the resolution of the outputted PNGs
CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs automated analysis over the outputs produced by "
                                                 "`generate_outputs.py`, and optionally uploads the outputs to Drive.")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file",
                        help="Path to the pipeline configuration json file")

    parser.add_argument("messages_json_input_path", metavar="messages-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("individuals_json_input_path", metavar="individuals-json-input-path",
                        help="Path to a JSONL file to read the TracedData of the messages data from")
    parser.add_argument("automated_analysis_output_dir", metavar="automated-analysis-output-dir",
                        help="Directory to write the automated analysis outputs to")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path

    messages_json_input_path = args.messages_json_input_path
    individuals_json_input_path = args.individuals_json_input_path
    automated_analysis_output_dir = args.automated_analysis_output_dir

    IOUtils.ensure_dirs_exist(automated_analysis_output_dir)
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/maps/counties")
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/maps/constituencies")
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/maps/urban")
    IOUtils.ensure_dirs_exist(f"{automated_analysis_output_dir}/graphs")

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    # Read the messages dataset
    log.info(f"Loading the messages dataset from {messages_json_input_path}...")
    with open(messages_json_input_path) as f:
        messages = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(messages)} messages")

    # Read the individuals dataset
    log.info(f"Loading the individuals dataset from {individuals_json_input_path}...")
    with open(individuals_json_input_path) as f:
        individuals = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
    log.info(f"Loaded {len(individuals)} individuals")

    # Compute the number of messages, individuals, and relevant messages per episode and overall.
    log.info("Computing the per-episode and per-season engagement counts...")
    engagement_counts = OrderedDict()  # of episode name to counts
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        engagement_counts[plan.dataset_name] = {
            "Episode": plan.dataset_name,

            "Total Messages": "-",  # Can't report this for individual weeks because the data has been overwritten with "STOP"
            "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Labelled Messages": len(AnalysisUtils.filter_fully_labelled(messages, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, [plan])),

            "Total Participants": "-",
            "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, [plan])),
            "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, [plan]))
        }
    engagement_counts["Total"] = {
        "Episode": "Total",

        "Total Messages": len(messages),
        "Total Messages with Opt-Ins": len(AnalysisUtils.filter_opt_ins(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Labelled Messages": len(AnalysisUtils.filter_partially_labelled(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Messages": len(AnalysisUtils.filter_relevant(messages, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),

        "Total Participants": len(individuals),
        "Total Participants with Opt-Ins": len(AnalysisUtils.filter_opt_ins(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS)),
        "Total Relevant Participants": len(AnalysisUtils.filter_relevant(individuals, CONSENT_WITHDRAWN_KEY, PipelineConfiguration.RQA_CODING_PLANS))
    }

    with open(f"{automated_analysis_output_dir}/engagement_counts.csv", "w") as f:
        headers = [
            "Episode",
            "Total Messages", "Total Messages with Opt-Ins", "Total Labelled Messages", "Total Relevant Messages",
            "Total Participants", "Total Participants with Opt-Ins", "Total Relevant Participants"
        ]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in engagement_counts.values():
            writer.writerow(row)

    log.info("Computing the participation frequencies...")
    repeat_participations = OrderedDict()
    for i in range(1, len(PipelineConfiguration.RQA_CODING_PLANS) + 1):
        repeat_participations[i] = {
            "Episodes Participated In": i,
            "Number of Individuals": 0,
            "% of Individuals": None
        }

    # Compute the number of individuals who participated each possible number of times, from 1 to <number of RQAs>
    # An individual is considered to have participated if they sent a message and didn't opt-out, regardless of the
    # relevance of any of their messages.
    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.FALSE:
            weeks_participated = 0
            for plan in PipelineConfiguration.RQA_CODING_PLANS:
                if plan.raw_field in ind:
                    weeks_participated += 1
            assert weeks_participated != 0, f"Found individual '{ind['uid']}' with no participation in any week"
            repeat_participations[weeks_participated]["Number of Individuals"] += 1

    # Compute the percentage of individuals who participated each possible number of times.
    # Percentages are computed after excluding individuals who opted out.
    total_individuals = len([td for td in individuals if td["consent_withdrawn"] == Codes.FALSE])
    for rp in repeat_participations.values():
        rp["% of Individuals"] = round(rp["Number of Individuals"] / total_individuals * 100, 1)

    # Export the participation frequency data to a csv
    with open(f"{automated_analysis_output_dir}/repeat_participations.csv", "w") as f:
        headers = ["Episodes Participated In", "Number of Individuals", "% of Individuals"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for row in repeat_participations.values():
            writer.writerow(row)

    log.info("Computing the demographic distributions...")
    # Count the number of individuals with each demographic code.
    # This count excludes individuals who withdrew consent. STOP codes in each scheme are not exported, as it would look
    # like 0 individuals opted out otherwise, which could be confusing.
    demographic_distributions = OrderedDict()  # of analysis_file_key -> code id -> number of individuals
    total_relevant = OrderedDict()  # of analysis_file_key -> number of relevant individuals
    for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            demographic_distributions[cc.analysis_file_key] = OrderedDict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                demographic_distributions[cc.analysis_file_key][code.code_id] = 0
            total_relevant[cc.analysis_file_key] = 0

    for ind in individuals:
        if ind["consent_withdrawn"] == Codes.TRUE:
            continue

        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                assert cc.coding_mode == CodingModes.SINGLE
                code = cc.code_scheme.get_code_with_code_id(ind[cc.coded_field]["CodeID"])
                demographic_distributions[cc.analysis_file_key][code.code_id] += 1
                if code.code_type == CodeTypes.NORMAL:
                    total_relevant[cc.analysis_file_key] += 1

    with open(f"{automated_analysis_output_dir}/demographic_distributions.csv", "w") as f:
        headers = ["Demographic", "Code", "Participants with Opt-Ins", "Percent"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for i, code in enumerate(cc.code_scheme.codes):
                    # Don't export a row for STOP codes because these have already been excluded, so would
                    # report 0 here, which could be confusing.
                    if code.control_code == Codes.STOP:
                        continue

                    participants_with_opt_ins = demographic_distributions[cc.analysis_file_key][code.code_id]
                    row = {
                        "Demographic": cc.analysis_file_key if i == 0 else "",
                        "Code": code.string_value,
                        "Participants with Opt-Ins": participants_with_opt_ins,
                    }

                    # Only compute a percentage for relevant codes.
                    if code.code_type == CodeTypes.NORMAL:
                        row["Percent"] = round(participants_with_opt_ins / total_relevant[cc.analysis_file_key] * 100, 1)
                    else:
                        row["Percent"] = ""

                    writer.writerow(row)

    # Compute the theme distributions
    log.info("Computing the theme distributions...")

    def make_survey_counts_dict():
        survey_counts = OrderedDict()
        survey_counts["Total Participants"] = 0
        survey_counts["Total Participants %"] = None
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue  # Ignore STOP codes because we already excluded everyone who opted out.
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] = 0
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = None

        return survey_counts

    def update_survey_counts(survey_counts, td):
        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                if cc.coding_mode == CodingModes.SINGLE:
                    codes = [cc.code_scheme.get_code_with_code_id(td[cc.coded_field]["CodeID"])]
                else:
                    assert cc.coding_mode == CodingModes.MULTIPLE
                    codes = [cc.code_scheme.get_code_with_code_id(label["CodeID"]) for label in td[cc.coded_field]]

                for code in codes:
                    if code.control_code == Codes.STOP:
                        continue
                    survey_counts[f"{cc.analysis_file_key}:{code.string_value}"] += 1

    def set_survey_percentages(survey_counts, total_survey_counts):
        if total_survey_counts["Total Participants"] == 0:
            survey_counts["Total Participants %"] = "-"
        else:
            survey_counts["Total Participants %"] = \
                round(survey_counts["Total Participants"] / total_survey_counts["Total Participants"] * 100, 1)

        for plan in PipelineConfiguration.SURVEY_CODING_PLANS:
            for cc in plan.coding_configurations:
                if cc.analysis_file_key is None:
                    continue

                for code in cc.code_scheme.codes:
                    if code.control_code == Codes.STOP:
                        continue

                    code_count = survey_counts[f"{cc.analysis_file_key}:{code.string_value}"]
                    code_total = total_survey_counts[f"{cc.analysis_file_key}:{code.string_value}"]

                    if code_total == 0:
                        survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = "-"
                    else:
                        survey_counts[f"{cc.analysis_file_key}:{code.string_value} %"] = \
                            round(code_count / code_total * 100, 1)

    episodes = OrderedDict()
    for episode_plan in PipelineConfiguration.RQA_CODING_PLANS:
        # Prepare empty counts of the survey responses for each variable
        themes = OrderedDict()
        episodes[episode_plan.raw_field] = themes
        for cc in episode_plan.coding_configurations:
            # TODO: Add support for CodingModes.SINGLE if we need it e.g. for IMAQAL?
            assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"
            themes["Total Relevant Participants"] = make_survey_counts_dict()
            for code in cc.code_scheme.codes:
                if code.control_code == Codes.STOP:
                    continue
                themes[f"{cc.analysis_file_key}{code.string_value}"] = make_survey_counts_dict()

        # Fill in the counts by iterating over every individual
        for td in individuals:
            if td["consent_withdrawn"] == Codes.TRUE:
                continue

            relevant_participant = False
            for cc in episode_plan.coding_configurations:
                assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"
                for label in td[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                    if code.control_code == Codes.STOP:
                        continue
                    themes[f"{cc.analysis_file_key}{code.string_value}"]["Total Participants"] += 1
                    update_survey_counts(themes[f"{cc.analysis_file_key}{code.string_value}"], td)
                    if code.code_type == CodeTypes.NORMAL:
                        relevant_participant = True

            if relevant_participant:
                themes["Total Relevant Participants"]["Total Participants"] += 1
                update_survey_counts(themes["Total Relevant Participants"], td)

        set_survey_percentages(themes["Total Relevant Participants"], themes["Total Relevant Participants"])

        for cc in episode_plan.coding_configurations:
            assert cc.coding_mode == CodingModes.MULTIPLE, "Other CodingModes not (yet) supported"

            for code in cc.code_scheme.codes:
                if code.code_type != CodeTypes.NORMAL:
                    continue

                theme = themes[f"{cc.analysis_file_key}{code.string_value}"]
                set_survey_percentages(theme, themes["Total Relevant Participants"])

    with open(f"{automated_analysis_output_dir}/theme_distributions.csv", "w") as f:
        headers = ["Question", "Variable"] + list(make_survey_counts_dict().keys())
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        last_row_episode = None
        for episode, themes in episodes.items():
            for theme, survey_counts in themes.items():
                row = {
                    "Question": episode if episode != last_row_episode else "",
                    "Variable": theme,
                }
                row.update(survey_counts)
                writer.writerow(row)
                last_row_episode = episode

    # Export a random sample of 100 messages for each normal code
    log.info("Exporting samples of up to 100 messages for each normal code...")
    samples = []  # of dict
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        for cc in plan.coding_configurations:
            code_to_messages = dict()
            for code in cc.code_scheme.codes:
                code_to_messages[code.string_value] = []

            for msg in messages:
                if not AnalysisUtils.opt_in(msg, CONSENT_WITHDRAWN_KEY, plan):
                    continue

                for label in msg[cc.coded_field]:
                    code = cc.code_scheme.get_code_with_code_id(label["CodeID"])
                    code_to_messages[code.string_value].append(msg[plan.raw_field])

            for code_string_value in code_to_messages:
                # Sample for at most 100 messages (note: this will give a different sample on each pipeline run)
                sample_size = min(100, len(code_to_messages[code_string_value]))
                sample_messages = random.sample(code_to_messages[code_string_value], sample_size)

                for msg in sample_messages:
                    samples.append({
                        "Episode": plan.dataset_name,
                        "Code Scheme": cc.code_scheme.name,
                        "Code": code_string_value,
                        "Sample Message": msg
                    })

    with open(f"{automated_analysis_output_dir}/sample_messages.csv", "w") as f:
        headers = ["Episode", "Code Scheme", "Code", "Sample Message"]
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()

        for sample in samples:
            writer.writerow(sample)

    # Produce maps of Kenya at county level
    log.info("Loading the Kenya county geojson...")
    counties_map = geopandas.read_file("geojson/kenya_counties.geojson")

    log.info("Loading the Kenya lakes geojson...")
    lakes_map = geopandas.read_file("geojson/kenya_lakes.geojson")
    # Keep only Kenya's great lakes
    lakes_map = lakes_map[lakes_map.LAKE_AVF.isin({"lake_turkana", "lake_victoria"})]

    log.info("Generating a map of per-county participation for the season")
    county_frequencies = dict()
    labels = dict()
    for code in CodeSchemes.KENYA_COUNTY.codes:
        if code.code_type == CodeTypes.NORMAL:
            county_frequencies[code.string_value] = demographic_distributions["county"][code.code_id]
            labels[code.string_value] = county_frequencies[code.string_value]
    
    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(counties_map, "ADM1_AVF", county_frequencies, ax=ax,
                                    labels=labels, label_position_columns=("ADM1_LX", "ADM1_LY"),
                                    callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"))
    MappingUtils.plot_water_bodies(lakes_map, ax=ax)
    fig.savefig(f"{automated_analysis_output_dir}/maps/counties/county_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close(fig)

    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]

        for cc in plan.coding_configurations:
            # Plot a map of the total relevant participants for this coding configuration.
            rqa_total_county_frequencies = dict()
            labels = dict()
            for county_code in CodeSchemes.KENYA_COUNTY.codes:
                if county_code.code_type == CodeTypes.NORMAL:
                    rqa_total_county_frequencies[county_code.string_value] = \
                        episode["Total Relevant Participants"][f"county:{county_code.string_value}"]
                    labels[county_code.string_value] = rqa_total_county_frequencies[county_code.string_value]

            fig, ax = plt.subplots()
            MappingUtils.plot_frequency_map(counties_map, "ADM1_AVF", rqa_total_county_frequencies, ax=ax,
                                            labels=labels, label_position_columns=("ADM1_LX", "ADM1_LY"),
                                            callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"))
            MappingUtils.plot_water_bodies(lakes_map, ax=ax)
            fig.savefig(f"{automated_analysis_output_dir}/maps/counties/county_{cc.analysis_file_key}total_relevant.png",
                        dpi=1200, bbox_inches="tight")
            plt.close(fig)

            # Plot maps of each of the normal themes for this coding configuration.
            map_index = 1
            for code in cc.code_scheme.codes:
                if code.code_type != CodeTypes.NORMAL:
                    continue

                theme = f"{cc.analysis_file_key}{code.string_value}"
                log.info(f"Generating a map of per-county participation for {theme}...")
                demographic_counts = episode[theme]

                theme_county_frequencies = dict()
                for county_code in CodeSchemes.KENYA_COUNTY.codes:
                    if county_code.code_type == CodeTypes.NORMAL:
                        theme_county_frequencies[county_code.string_value] = \
                            demographic_counts[f"county:{county_code.string_value}"]

                fig, ax = plt.subplots()
                MappingUtils.plot_frequency_map(counties_map, "ADM1_AVF", theme_county_frequencies, ax=ax,
                                                label_position_columns=("ADM1_LX", "ADM1_LY"),
                                                callout_position_columns=("ADM1_CALLX", "ADM1_CALLY"))
                MappingUtils.plot_water_bodies(lakes_map, ax=ax)
                fig.savefig(f"{automated_analysis_output_dir}/maps/counties/county_{cc.analysis_file_key}{map_index}_{code.string_value}.png",
                            dpi=1200, bbox_inches="tight")
                plt.close(fig)

                map_index += 1

    # Produce maps of Kenya at constituency level
    log.info("Loading the Kenya constituency geojson...")
    constituencies_map = geopandas.read_file("geojson/kenya_constituencies.geojson")

    log.info("Generating a map of per-constituency participation for the season")
    constituency_frequencies = dict()
    for code in CodeSchemes.KENYA_CONSTITUENCY.codes:
        if code.code_type == CodeTypes.NORMAL:
            constituency_frequencies[code.string_value] = demographic_distributions["constituency"][code.code_id]

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(constituencies_map, "ADM2_AVF", constituency_frequencies, ax=ax)
    MappingUtils.plot_inset_frequency_map(
        constituencies_map, "ADM2_AVF", constituency_frequencies,
        inset_region=(36.62, -1.46, 37.12, -1.09), zoom=3, inset_position=(35.60, -2.95), ax=ax)
    MappingUtils.plot_water_bodies(lakes_map, ax=ax)
    plt.savefig(f"{automated_analysis_output_dir}/maps/constituencies/constituency_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close(fig)

    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]

        for cc in plan.coding_configurations:
            # Plot a map of the total relevant participants for this coding configuration.
            rqa_total_constituency_frequencies = dict()
            for constituency_code in CodeSchemes.KENYA_CONSTITUENCY.codes:
                if constituency_code.code_type == CodeTypes.NORMAL:
                    rqa_total_constituency_frequencies[constituency_code.string_value] = \
                        episode["Total Relevant Participants"][f"constituency:{constituency_code.string_value}"]

            fig, ax = plt.subplots()
            MappingUtils.plot_frequency_map(constituencies_map, "ADM2_AVF", rqa_total_constituency_frequencies, ax=ax)
            MappingUtils.plot_inset_frequency_map(
                constituencies_map, "ADM2_AVF", rqa_total_constituency_frequencies,
                inset_region=(36.62, -1.46, 37.12, -1.09), zoom=3, inset_position=(35.60, -2.95), ax=ax)
            MappingUtils.plot_water_bodies(lakes_map, ax=ax)
            plt.savefig(f"{automated_analysis_output_dir}/maps/constituencies/constituency_{cc.analysis_file_key}total_relevant.png",
                        dpi=1200, bbox_inches="tight")
            plt.close(fig)

    # Produce maps of Nairobi/Kiambu at constituency level
    log.info("Loading the Kenya constituency geojson...")
    constituencies_map = geopandas.read_file("geojson/kenya_constituencies.geojson")
    urban_map = constituencies_map[constituencies_map.ADM1_AVF.isin({KenyaCodes.NAIROBI, KenyaCodes.KIAMBU})]

    # Constituencies to label with their name, as requested by RDA for COVID19-KE-Urban
    constituencies_to_label_with_name = {
        # TODO: Switch to use KenyaCodes instead of strings
        "kibra", "mathare", "embakasi_east", "embakasi_central", "kasarani",  # requested because urban-poor targets
        "ruiru", "kikuyu", "kiambu"  # requested due to high participation
    }

    constituency_display_names = dict()  # of constituency id -> constituency name to display
    for i, admin_region in constituencies_map.iterrows():
        constituency_display_names[admin_region.ADM2_AVF] = admin_region.ADM2_EN

    log.info("Generating a map of participation in Nairobi/Kiambu for the season")
    urban_frequencies = dict()
    labels = dict()
    for code in CodeSchemes.KENYA_CONSTITUENCY.codes:
        if code.code_type == CodeTypes.NORMAL:
            urban_frequencies[code.string_value] = demographic_distributions["constituency"][code.code_id]

            if code.string_value in constituencies_to_label_with_name:
                constituency_name = constituency_display_names[code.string_value]
                labels[code.string_value] = constituency_name + "\n" + str(urban_frequencies[code.string_value])
            else:
                labels[code.string_value] = str(urban_frequencies[code.string_value])

    fig, ax = plt.subplots()
    MappingUtils.plot_frequency_map(urban_map, "ADM2_AVF", urban_frequencies, ax=ax,
                                    labels=labels, label_position_columns=("ADM2_LX", "ADM2_LY"),
                                    callout_position_columns=("ADM2_CALLX", "ADM2_CALLY"))
    fig.savefig(f"{automated_analysis_output_dir}/maps/urban/urban_total_participants.png", dpi=1200, bbox_inches="tight")
    plt.close(fig)

    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]

        for cc in plan.coding_configurations:
            # Plot a map of the total relevant participants for this coding configuration.
            rqa_total_urban_frequencies = dict()
            labels = dict()
            for code in CodeSchemes.KENYA_CONSTITUENCY.codes:
                if code.code_type == CodeTypes.NORMAL:
                    rqa_total_urban_frequencies[code.string_value] = \
                        episode["Total Relevant Participants"][f"constituency:{code.string_value}"]

                    if code.string_value in constituencies_to_label_with_name:
                        constituency_name = constituency_display_names[code.string_value]
                        labels[code.string_value] = constituency_name + "\n" + str(rqa_total_urban_frequencies[code.string_value])
                    else:
                        labels[code.string_value] = str(rqa_total_urban_frequencies[code.string_value])

            fig, ax = plt.subplots()
            MappingUtils.plot_frequency_map(urban_map, "ADM2_AVF", rqa_total_urban_frequencies, ax=ax,
                                            labels=labels, label_position_columns=("ADM2_LX", "ADM2_LY"),
                                            callout_position_columns=("ADM2_CALLX", "ADM2_CALLY"))
            plt.savefig(f"{automated_analysis_output_dir}/maps/urban/urban_{cc.analysis_file_key}total_relevant.png",
                        dpi=1200, bbox_inches="tight")
            plt.close(fig)

    log.info("Graphing the per-episode engagement counts...")
    # Graph the number of messages in each episode
    fig = px.bar([x for x in engagement_counts.values() if x["Episode"] != "Total"],
                 x="Episode", y="Total Messages with Opt-Ins", template="plotly_white",
                 title="Messages/Episode", width=len(engagement_counts) * 20 + 150)
    fig.update_xaxes(tickangle=-60)
    fig.write_image(f"{automated_analysis_output_dir}/graphs/messages_per_episode.png", scale=IMG_SCALE_FACTOR)

    # Graph the number of participants in each episode
    fig = px.bar([x for x in engagement_counts.values() if x["Episode"] != "Total"],
                 x="Episode", y="Total Participants with Opt-Ins", template="plotly_white",
                 title="Participants/Episode", width=len(engagement_counts) * 20 + 150)
    fig.update_xaxes(tickangle=-60)
    fig.write_image(f"{automated_analysis_output_dir}/graphs/participants_per_episode.png", scale=IMG_SCALE_FACTOR)

    log.info("Graphing the demographic distributions...")
    for plan in PipelineConfiguration.DEMOG_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            if len(cc.code_scheme.codes) > 200:
                log.warning(f"Skipping graphing the distribution of codes for {cc.analysis_file_key}, because it "
                            f"contains too many columns to graph (has {len(cc.code_scheme.codes)} columns; "
                            f"limit is 200).")
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            fig = px.bar([{"Label": code.string_value,
                           "Number of Participants": demographic_distributions[cc.analysis_file_key][code.code_id]}
                          for code in cc.code_scheme.codes if code.control_code != Codes.STOP],
                         x="Label", y="Number of Participants", template="plotly_white",
                         title=f"Season Distribution: {cc.analysis_file_key}", width=len(cc.code_scheme.codes) * 20 + 150)
            fig.update_xaxes(type="category", tickangle=-60, dtick=1)
            fig.write_image(f"{automated_analysis_output_dir}/graphs/season_distribution_{cc.analysis_file_key}.png", scale=IMG_SCALE_FACTOR)

    # Plot the per-season distribution of responses for each survey question, per individual
    for plan in PipelineConfiguration.RQA_CODING_PLANS + PipelineConfiguration.SURVEY_CODING_PLANS:
        for cc in plan.coding_configurations:
            if cc.analysis_file_key is None:
                continue

            # Don't generate graphs for the demographics, as they were already generated above.
            # TODO: Update the demographic_distributions to include the distributions for all variables?
            if cc.analysis_file_key in demographic_distributions:
                continue

            log.info(f"Graphing the distribution of codes for {cc.analysis_file_key}...")
            label_counts = OrderedDict()
            for code in cc.code_scheme.codes:
                label_counts[code.string_value] = 0

            if cc.coding_mode == CodingModes.SINGLE:
                for ind in individuals:
                    label_counts[ind[cc.analysis_file_key]] += 1
            else:
                assert cc.coding_mode == CodingModes.MULTIPLE
                for ind in individuals:
                    for code in cc.code_scheme.codes:
                        if ind[f"{cc.analysis_file_key}{code.string_value}"] == Codes.MATRIX_1:
                            label_counts[code.string_value] += 1

            data = [{"Label": k, "Number of Participants": v} for k, v in label_counts.items()]
            fig = px.bar(data, x="Label", y="Number of Participants", template="plotly_white",
                         title=f"Season Distribution: {cc.analysis_file_key}", width=len(label_counts) * 20 + 150)
            fig.update_xaxes(tickangle=-60)
            fig.write_image(f"{automated_analysis_output_dir}/graphs/season_distribution_{cc.analysis_file_key}.png", scale=IMG_SCALE_FACTOR)

    log.info("Graphing pie chart of normal codes for gender...")
    # TODO: Gender is hard-coded here for COVID19. If we need this in future, but don't want to extend to other
    #       demographic variables, then this will need to be controlled from configuration
    gender_distribution = demographic_distributions["gender"]
    normal_gender_distribution = []
    for code in CodeSchemes.GENDER.codes:
        if code.code_type == CodeTypes.NORMAL:
            normal_gender_distribution.append({
                "Gender": code.string_value,
                "Number of Participants": gender_distribution[code.code_id]
            })
    fig = px.pie(normal_gender_distribution, names="Gender", values="Number of Participants",
                 title="Season Distribution: gender", template="plotly_white")
    fig.update_traces(textinfo="value")
    fig.write_image(f"{automated_analysis_output_dir}/graphs/season_distribution_gender_pie.png", scale=IMG_SCALE_FACTOR)

    log.info("Graphing normal themes by gender...")
    # Adapt the theme distributions produced above to extract the normal RQA + gender codes, and graph by gender
    # TODO: Gender is hard-coded here for COVID19. If we need this in future, but don't want to extend to other
    #       demographic variables, then this will need to be controlled from configuration
    for plan in PipelineConfiguration.RQA_CODING_PLANS:
        episode = episodes[plan.raw_field]
        normal_themes = dict()

        for cc in plan.coding_configurations:
            for code in cc.code_scheme.codes:
                if code.code_type == CodeTypes.NORMAL and code.string_value not in {"knowledge", "attitude", "behaviour"}:
                    normal_themes[code.string_value] = episode[f"{cc.analysis_file_key}{code.string_value}"]

        if len(normal_themes) == 0:
            log.warning(f"Skipping graphing normal themes by gender for {plan.raw_field} because the scheme does "
                        f"not contain any normal codes")
            continue

        normal_by_gender = []
        for theme, demographic_counts in normal_themes.items():
            for gender_code in CodeSchemes.GENDER.codes:
                if gender_code.code_type != CodeTypes.NORMAL:
                    continue

                total_relevant_gender = episode["Total Relevant Participants"][f"gender:{gender_code.string_value}"]
                normal_by_gender.append({
                    "RQA Theme": theme,
                    "Gender": gender_code.string_value,
                    "Number of Participants": demographic_counts[f"gender:{gender_code.string_value}"],
                    "Fraction of Relevant Participants": None if total_relevant_gender == 0 else
                        demographic_counts[f"gender:{gender_code.string_value}"] / total_relevant_gender
                })

        fig = px.bar(normal_by_gender, x="RQA Theme", y="Number of Participants", color="Gender", barmode="group",
                     template="plotly_white")
        fig.update_layout(title_text=f"{plan.raw_field} by gender (absolute)")
        fig.update_xaxes(tickangle=-60)
        fig.write_image(f"{automated_analysis_output_dir}/graphs/{plan.raw_field}_by_gender_absolute.png", scale=IMG_SCALE_FACTOR)

        fig = px.bar(normal_by_gender, x="RQA Theme", y="Fraction of Relevant Participants", color="Gender", barmode="group",
                     template="plotly_white")
        fig.update_layout(title_text=f"{plan.raw_field} by gender (normalised)")
        fig.update_xaxes(tickangle=-60)
        fig.write_image(f"{automated_analysis_output_dir}/graphs/{plan.raw_field}_by_gender_normalised.png", scale=IMG_SCALE_FACTOR)
