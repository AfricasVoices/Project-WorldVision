import argparse
import json
import os
from glob import glob

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

from src.lib import PipelineConfiguration

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads analysis output files to Google Drive")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("pipeline_configuration_file_path", metavar="pipeline-configuration-file-path",
                        help="Path to the pipeline configuration json file")
    parser.add_argument("run_id", metavar="run-id",
                        help="Identifier of this pipeline run")
    parser.add_argument("production_csv_input_path", metavar="production-csv-input-path",
                        help="Path to a CSV file with raw message and demographic response, for use in "
                             "radio show production"),
    parser.add_argument("messages_csv_input_path", metavar="messages-csv-input-path",
                        help="Path to analysis dataset CSV where messages are the unit for analysis (i.e. one message "
                             "per row)"),
    parser.add_argument("individuals_csv_input_path", metavar="individuals-csv-input-path",
                        help="Path to analysis dataset CSV where respondents are the unit for analysis (i.e. one "
                             "respondent per row, with all their messages joined into a single cell)")
    parser.add_argument("automated_analysis_input_dir", metavar="automated-analysis-input-dir",
                        help="Directory to read the automated analysis outputs from")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_configuration_file_path = args.pipeline_configuration_file_path
    run_id = args.run_id
    production_csv_input_path = args.production_csv_input_path
    messages_csv_input_path = args.messages_csv_input_path
    individuals_csv_input_path = args.individuals_csv_input_path
    automated_analysis_input_dir = args.automated_analysis_input_dir

    log.info("Loading Pipeline Configuration File...")
    with open(pipeline_configuration_file_path) as f:
        pipeline_configuration = PipelineConfiguration.from_configuration_file(f)
    Logger.set_project_name(pipeline_configuration.pipeline_name)
    log.debug(f"Pipeline name is {pipeline_configuration.pipeline_name}")

    # Upload to Google Drive, if requested.
    if pipeline_configuration.drive_upload is not None:
        log.info(f"Downloading Google Drive service account credentials...")
        credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, pipeline_configuration.drive_upload.drive_credentials_file_url))
        drive_client_wrapper.init_client_from_info(credentials_info)

        log.info("Uploading analysis CSVs to Google Drive...")
        production_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.production_upload_path)
        production_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.production_upload_path)
        drive_client_wrapper.update_or_create(production_csv_input_path, production_csv_drive_dir,
                                              target_file_name=production_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True, recursive=True)

        messages_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.messages_upload_path)
        messages_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.messages_upload_path)
        drive_client_wrapper.update_or_create(messages_csv_input_path, messages_csv_drive_dir,
                                              target_file_name=messages_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True, recursive=True)

        individuals_csv_drive_dir = os.path.dirname(pipeline_configuration.drive_upload.individuals_upload_path)
        individuals_csv_drive_file_name = os.path.basename(pipeline_configuration.drive_upload.individuals_upload_path)
        drive_client_wrapper.update_or_create(individuals_csv_input_path, individuals_csv_drive_dir,
                                              target_file_name=individuals_csv_drive_file_name,
                                              target_folder_is_shared_with_me=True, recursive=True)

        paths_to_upload = glob(f"{automated_analysis_input_dir}/*.csv")
        log.info(f"Uploading {len(paths_to_upload)} CSVs to Drive...")
        drive_client_wrapper.update_or_create_batch(
            paths_to_upload, pipeline_configuration.drive_upload.analysis_graphs_dir,
            target_folder_is_shared_with_me=True, recursive=True
        )

        paths_to_upload = glob(f"{automated_analysis_input_dir}/graphs/*.png")
        log.info(f"Uploading {len(paths_to_upload)} graphs to Drive...")
        drive_client_wrapper.update_or_create_batch(
            paths_to_upload, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/graphs",
            target_folder_is_shared_with_me=True, recursive=True
        )

        paths_to_upload = glob(f"{automated_analysis_input_dir}/maps/counties/*.png")
        log.info(f"Uploading {len(paths_to_upload)} county maps to Drive...")
        drive_client_wrapper.update_or_create_batch(
            paths_to_upload, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/maps/counties",
            target_folder_is_shared_with_me=True, recursive=True
        )

        paths_to_upload = glob(f"{automated_analysis_input_dir}/maps/constituencies/*.png")
        log.info(f"Uploading {len(paths_to_upload)} constituency maps to Drive...")
        drive_client_wrapper.update_or_create_batch(
            paths_to_upload, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/maps/constituencies",
            target_folder_is_shared_with_me=True, recursive=True
        )
        paths_to_upload = glob(f"{automated_analysis_input_dir}/maps/urban/*.png")
        log.info(f"Uploading {len(paths_to_upload)} urban maps to Drive...")
        drive_client_wrapper.update_or_create_batch(
            paths_to_upload, f"{pipeline_configuration.drive_upload.analysis_graphs_dir}/maps/urban",
            target_folder_is_shared_with_me=True, recursive=True
        )
    else:
        log.info("Skipping uploading to Google Drive (because the pipeline configuration json does not contain the key "
                 "'DriveUploadPaths')")
