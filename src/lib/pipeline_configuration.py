import json
from abc import ABC, abstractmethod
from datetime import datetime
from urllib.parse import urlparse

import pytz
from core_data_modules.data_models import validators
from dateutil.parser import isoparse

from configuration import coding_plans


class PipelineConfiguration(object):
    RQA_CODING_PLANS = []
    DEMOG_CODING_PLANS = []
    SURVEY_CODING_PLANS = []
    WS_CORRECT_DATASET_SCHEME = None
    
    def __init__(self, pipeline_name, raw_data_sources, phone_number_uuid_table, timestamp_remappings,
                 rapid_pro_key_remappings, project_start_date, project_end_date, filter_test_messages, move_ws_messages,
                 memory_profile_upload_bucket, data_archive_upload_bucket, bucket_dir_path, drive_upload=None):
        """
        :param pipeline_name: The name of this pipeline.
        :type pipeline_name: str
        :param raw_data_sources: List of sources to pull the various raw run files from.
        :type raw_data_sources: list of RawDataSource
        :param phone_number_uuid_table: Configuration for the Firestore phone number <-> uuid table.
        :type phone_number_uuid_table: PhoneNumberUuidTable
        :param rapid_pro_key_remappings: List of rapid_pro_key -> pipeline_key remappings.
        :type rapid_pro_key_remappings: list of RapidProKeyRemapping
        :param project_start_date: When data collection started - all activation messages received before this date
                                   time will be dropped.
        :type project_start_date: datetime.datetime
        :param project_end_date: When data collection stopped - all activation messages received on or after this date
                                 time will be dropped.
        :type project_end_date: datetime.datetime
        :param filter_test_messages: Whether to filter out messages sent from the rapid_pro_test_contact_uuids
        :type filter_test_messages: bool
        :param move_ws_messages: Whether to move messages labelled as Wrong Scheme to the correct dataset.
        :type move_ws_messages: bool
        :param memory_profile_upload_bucket: The GS bucket name to upload the memory profile log to.
                                             This name will be appended with the log_dir_path
                                             and the file basename to generate the log upload location.
        :type memory_profile_upload_bucket: str
        :param data_archive_upload_bucket: The GS bucket name to upload the data archive file to.
                                           This name will be appended with the log_dir_path
                                           and the file basename to generate the archive upload location.
        :type data_archive_upload_bucket: str
        :param bucket_dir_path: The GS bucket folder path to store the data archive & memory log files to.
        :type bucket_dir_path: str
        :param drive_upload: Configuration for uploading to Google Drive, or None.
                             If None, does not upload to Google Drive.
        :type drive_upload: DriveUploadPaths | None
        """
        self.pipeline_name = pipeline_name
        self.raw_data_sources = raw_data_sources
        self.phone_number_uuid_table = phone_number_uuid_table
        self.timestamp_remappings = timestamp_remappings
        self.rapid_pro_key_remappings = rapid_pro_key_remappings
        self.project_start_date = project_start_date
        self.project_end_date = project_end_date
        self.filter_test_messages = filter_test_messages
        self.move_ws_messages = move_ws_messages
        self.drive_upload = drive_upload
        self.memory_profile_upload_bucket = memory_profile_upload_bucket
        self.data_archive_upload_bucket = data_archive_upload_bucket
        self.bucket_dir_path = bucket_dir_path

        PipelineConfiguration.RQA_CODING_PLANS = coding_plans.get_rqa_coding_plans(self.pipeline_name)

        PipelineConfiguration.DEMOG_CODING_PLANS = coding_plans.get_demog_coding_plans(self.pipeline_name)
        PipelineConfiguration.FOLLOW_UP_CODING_PLANS = coding_plans.get_follow_up_coding_plans(self.pipeline_name)
        PipelineConfiguration.SURVEY_CODING_PLANS = \
            PipelineConfiguration.DEMOG_CODING_PLANS + PipelineConfiguration.FOLLOW_UP_CODING_PLANS

        PipelineConfiguration.WS_CORRECT_DATASET_SCHEME = coding_plans.get_ws_correct_dataset_scheme(self.pipeline_name)

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        pipeline_name = configuration_dict["PipelineName"]

        raw_data_sources = []
        for raw_data_source in configuration_dict["RawDataSources"]:
            if raw_data_source["SourceType"] == "RapidPro":
                raw_data_sources.append(RapidProSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "GCloudBucket":
                raw_data_sources.append(GCloudBucketSource.from_configuration_dict(raw_data_source))
            elif raw_data_source["SourceType"] == "RecoveryCSV":
                raw_data_sources.append(RecoveryCSVSource.from_configuration_dict(raw_data_source))
            else:
                assert False, f"Unknown SourceType '{raw_data_source['SourceType']}'. " \
                              f"Must be 'RapidPro', 'GCloudBucket', or 'RecoveryCSV'."

        phone_number_uuid_table = PhoneNumberUuidTable.from_configuration_dict(
            configuration_dict["PhoneNumberUuidTable"])

        timestamp_remappings = []
        for remapping_dict in configuration_dict.get("TimestampRemappings", []):
            timestamp_remappings.append(TimestampRemapping.from_configuration_dict(remapping_dict))

        rapid_pro_key_remappings = []
        for remapping_dict in configuration_dict["RapidProKeyRemappings"]:
            rapid_pro_key_remappings.append(RapidProKeyRemapping.from_configuration_dict(remapping_dict))

        project_start_date = isoparse(configuration_dict["ProjectStartDate"])
        project_end_date = isoparse(configuration_dict["ProjectEndDate"])

        filter_test_messages = configuration_dict["FilterTestMessages"]
        move_ws_messages = configuration_dict["MoveWSMessages"]

        drive_upload_paths = None
        if "DriveUpload" in configuration_dict:
            drive_upload_paths = DriveUpload.from_configuration_dict(configuration_dict["DriveUpload"])

        memory_profile_upload_bucket = configuration_dict["MemoryProfileUploadBucket"]
        data_archive_upload_bucket = configuration_dict["DataArchiveUploadBucket"]
        bucket_dir_path = configuration_dict["BucketDirPath"]

        return cls(pipeline_name, raw_data_sources, phone_number_uuid_table, timestamp_remappings,
                   rapid_pro_key_remappings, project_start_date, project_end_date, filter_test_messages,
                   move_ws_messages, memory_profile_upload_bucket, data_archive_upload_bucket, bucket_dir_path,
                   drive_upload_paths)

    @classmethod
    def from_configuration_file(cls, f):
        return cls.from_configuration_dict(json.load(f))

    def validate(self):
        validators.validate_string(self.pipeline_name, "pipeline_name")

        validators.validate_list(self.raw_data_sources, "raw_data_sources")
        for i, raw_data_source in enumerate(self.raw_data_sources):
            assert isinstance(raw_data_source, RawDataSource), f"raw_data_sources[{i}] is not of type of RawDataSource"
            raw_data_source.validate()

        assert isinstance(self.phone_number_uuid_table, PhoneNumberUuidTable)
        self.phone_number_uuid_table.validate()

        validators.validate_list(self.rapid_pro_key_remappings, "rapid_pro_key_remappings")
        for i, remapping in enumerate(self.rapid_pro_key_remappings):
            assert isinstance(remapping, RapidProKeyRemapping), \
                f"rapid_pro_key_mappings[{i}] is not of type RapidProKeyRemapping"
            remapping.validate()

        validators.validate_datetime(self.project_start_date, "project_start_date")
        validators.validate_datetime(self.project_end_date, "project_end_date")

        validators.validate_bool(self.filter_test_messages, "filter_test_messages")
        validators.validate_bool(self.move_ws_messages, "move_ws_messages")
        
        validators.validate_url(self.memory_profile_upload_bucket, "memory_profile_upload_bucket", "gs")
        validators.validate_url(self.data_archive_upload_bucket, "data_archive_upload_bucket", "gs")
        validators.validate_string(self.bucket_dir_path, "bucket_dir_path")

        if self.drive_upload is not None:
            assert isinstance(self.drive_upload, DriveUpload), \
                "drive_upload is not of type DriveUpload"
            self.drive_upload.validate()


class RawDataSource(ABC):
    @abstractmethod
    def get_activation_flow_names(self):
        pass

    @abstractmethod
    def get_survey_flow_names(self):
        pass

    @abstractmethod
    def validate(self):
        pass


class RapidProSource(RawDataSource):
    def __init__(self, source_name, domain, token_file_url, contacts_file_name, activation_flow_names,
                 survey_flow_names, test_contact_uuids):
        """
        :param domain: URL of the Rapid Pro server to download data from.
        :type domain: str
        :param token_file_url: GS URL of a text file containing the authorisation token for the Rapid Pro server.
        :type token_file_url: str
        :param contacts_file_name:
        :type contacts_file_name: str
        :param activation_flow_names: The names of the RapidPro flows that contain the radio show responses.
        :type: activation_flow_names: list of str
        :param survey_flow_names: The names of the RapidPro flows that contain the survey responses.
        :type: survey_flow_names: list of str
        :param test_contact_uuids: Rapid Pro contact UUIDs of test contacts.
                                   Runs for any of those test contacts will be tagged with {'test_run': True},
                                   and dropped when the pipeline is run with "FilterTestMessages" set to true.
        :type test_contact_uuids: list of str
        """
        self.source_name = source_name
        self.domain = domain
        self.token_file_url = token_file_url
        self.contacts_file_name = contacts_file_name
        self.activation_flow_names = activation_flow_names
        self.survey_flow_names = survey_flow_names
        self.test_contact_uuids = test_contact_uuids

        self.validate()

    def get_activation_flow_names(self):
        return self.activation_flow_names

    def get_survey_flow_names(self):
        return self.survey_flow_names

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        source_name = configuration_dict["SourceName"]
        domain = configuration_dict["Domain"]
        token_file_url = configuration_dict["TokenFileURL"]
        contacts_file_name = configuration_dict["ContactsFileName"]
        activation_flow_names = configuration_dict.get("ActivationFlowNames", [])
        survey_flow_names = configuration_dict.get("SurveyFlowNames", [])
        test_contact_uuids = configuration_dict.get("TestContactUUIDs", [])

        return cls(source_name, domain, token_file_url, contacts_file_name, activation_flow_names,
                   survey_flow_names, test_contact_uuids)

    def validate(self):
        validators.validate_string(self.source_name, "source_name")
        validators.validate_string(self.domain, "domain")
        validators.validate_string(self.token_file_url, "token_file_url")
        validators.validate_string(self.contacts_file_name, "contacts_file_name")

        validators.validate_list(self.activation_flow_names, "activation_flow_names")
        for i, activation_flow_name in enumerate(self.activation_flow_names):
            validators.validate_string(activation_flow_name, f"activation_flow_names[{i}]")

        validators.validate_list(self.survey_flow_names, "survey_flow_names")
        for i, survey_flow_name in enumerate(self.survey_flow_names):
            validators.validate_string(survey_flow_name, f"survey_flow_names[{i}]")

        validators.validate_list(self.test_contact_uuids, "test_contact_uuids")
        for i, contact_uuid in enumerate(self.test_contact_uuids):
            validators.validate_string(contact_uuid, f"test_contact_uuids[{i}]")


class AbstractRemoteURLSource(RawDataSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        self.activation_flow_urls = activation_flow_urls
        self.survey_flow_urls = survey_flow_urls

        self.validate()

    def get_activation_flow_names(self):
        return [url.split('/')[-1].split('.')[0] for url in self.activation_flow_urls]

    def get_survey_flow_names(self):
        return [url.split('/')[-1].split('.')[0] for url in self.survey_flow_urls]

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        activation_flow_urls = configuration_dict.get("ActivationFlowURLs", [])
        survey_flow_urls = configuration_dict.get("SurveyFlowURLs", [])

        return cls(activation_flow_urls, survey_flow_urls)

    def validate(self):
        validators.validate_list(self.activation_flow_urls, "activation_flow_urls")
        for i, activation_flow_url in enumerate(self.activation_flow_urls):
            validators.validate_url(activation_flow_url, f"activation_flow_urls[{i}]", "gs")

        validators.validate_list(self.survey_flow_urls, "survey_flow_urls")
        for i, survey_flow_url in enumerate(self.survey_flow_urls):
            validators.validate_url(survey_flow_url, f"survey_flow_urls[{i}]", "gs")


class GCloudBucketSource(AbstractRemoteURLSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        super().__init__(activation_flow_urls, survey_flow_urls)


class RecoveryCSVSource(AbstractRemoteURLSource):
    def __init__(self, activation_flow_urls, survey_flow_urls):
        super().__init__(activation_flow_urls, survey_flow_urls)


class PhoneNumberUuidTable(object):
    def __init__(self, firebase_credentials_file_url, table_name):
        """
        :param firebase_credentials_file_url: GS URL to the private credentials file for the Firebase account where
                                                 the phone number <-> uuid table is stored.
        :type firebase_credentials_file_url: str
        :param table_name: Name of the data <-> uuid table in Firebase to use.
        :type table_name: str
        """
        self.firebase_credentials_file_url = firebase_credentials_file_url
        self.table_name = table_name

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        firebase_credentials_file_url = configuration_dict["FirebaseCredentialsFileURL"]
        table_name = configuration_dict["TableName"]

        return cls(firebase_credentials_file_url, table_name)

    def validate(self):
        validators.validate_url(self.firebase_credentials_file_url, "firebase_credentials_file_url", scheme="gs")
        validators.validate_string(self.table_name, "table_name")


class TimestampRemapping(object):
    def __init__(self, time_key, show_pipeline_key_to_remap_to, range_start_inclusive=None, range_end_exclusive=None,
                 time_to_adjust_to=None):
        """
        Specifies a remapping of messages received within the given time range to another radio show field.
        Optionally specifies an adjustment of all affected timestamps to a constant datetime.

        :param time_key: Key in each TracedData of an ISO 8601-formatted datetime string to read the message sent on
                         time from.
        :type time_key: str
        :param show_pipeline_key_to_remap_to: Pipeline key to assign to messages received within the given time range.
        :type show_pipeline_key_to_remap_to: str
        :param range_start_inclusive: Start datetime for the time range to remap radio show messages from, inclusive.
                                      If None, defaults to the beginning of time.
        :type range_start_inclusive: datetime | None
        :param range_end_exclusive: End datetime for the time range to remap radio show messages from, exclusive.
                                    If None, defaults to the end of time.
        :type range_end_exclusive: datetime | None
        :param time_to_adjust_to: Datetime to adjust each message object's `time_key` field to, or None.
                                  If None, re-mapped shows will not have timestamps adjusted.
        :type time_to_adjust_to: datetime | None
        """
        if range_start_inclusive is None:
            range_start_inclusive = pytz.utc.localize(datetime.min)
        if range_end_exclusive is None:
            range_end_exclusive = pytz.utc.localize(datetime.max)

        self.time_key = time_key
        self.show_pipeline_key_to_remap_to = show_pipeline_key_to_remap_to
        self.range_start_inclusive = range_start_inclusive
        self.range_end_exclusive = range_end_exclusive
        self.time_to_adjust_to = time_to_adjust_to

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        time_key = configuration_dict["TimeKey"]
        show_pipeline_key_to_remap_to = configuration_dict["ShowPipelineKeyToRemapTo"]
        range_start_inclusive = configuration_dict.get("RangeStartInclusive")
        range_end_exclusive = configuration_dict.get("RangeEndExclusive")
        time_to_adjust_to = configuration_dict.get("TimeToAdjustTo")

        if range_start_inclusive is not None:
            range_start_inclusive = isoparse(range_start_inclusive)
        if range_end_exclusive is not None:
            range_end_exclusive = isoparse(range_end_exclusive)
        if time_to_adjust_to is not None:
            time_to_adjust_to = isoparse(time_to_adjust_to)

        return cls(time_key, show_pipeline_key_to_remap_to, range_start_inclusive, range_end_exclusive,
                   time_to_adjust_to)

    def validate(self):
        validators.validate_string(self.time_key, "time_key")
        validators.validate_string(self.show_pipeline_key_to_remap_to, "show_pipeline_key_to_remap_to")
        validators.validate_datetime(self.range_start_inclusive, "range_start_inclusive")
        validators.validate_datetime(self.range_end_exclusive, "range_end_exclusive")

        if self.time_to_adjust_to is not None:
            validators.validate_datetime(self.time_to_adjust_to, "time_to_adjust_to")


class RapidProKeyRemapping(object):
    def __init__(self, is_activation_message, rapid_pro_key, pipeline_key):
        """
        :param is_activation_message: Whether this re-mapping contains an activation message (activation messages need
                                   to be handled differently because they are not always in the correct flow)
        :type is_activation_message: bool
        :param rapid_pro_key: Name of key in the dataset exported via RapidProTools.
        :type rapid_pro_key: str
        :param pipeline_key: Name to use for that key in the rest of the pipeline.
        :type pipeline_key: str
        """
        self.is_activation_message = is_activation_message
        self.rapid_pro_key = rapid_pro_key
        self.pipeline_key = pipeline_key

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        is_activation_message = configuration_dict.get("IsActivationMessage", False)
        rapid_pro_key = configuration_dict["RapidProKey"]
        pipeline_key = configuration_dict["PipelineKey"]

        return cls(is_activation_message, rapid_pro_key, pipeline_key)

    def validate(self):
        validators.validate_bool(self.is_activation_message, "is_activation_message")
        validators.validate_string(self.rapid_pro_key, "rapid_pro_key")
        validators.validate_string(self.pipeline_key, "pipeline_key")


class DriveUpload(object):
    def __init__(self, drive_credentials_file_url, production_upload_path, messages_upload_path,
                 individuals_upload_path, analysis_graphs_dir):
        """
        :param drive_credentials_file_url: GS URL to the private credentials file for the Drive service account to use
                                           to upload the output files.
        :type drive_credentials_file_url: str
        :param production_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                       production CSV to.
        :type production_upload_path: str
        :param messages_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                     messages analysis CSV to.
        :type messages_upload_path: str
        :param individuals_upload_path: Path in the Drive service account's "Shared with Me" directory to upload the
                                        individuals analysis CSV to.
        :type individuals_upload_path: str
        :param analysis_graphs_dir: Directory in the Drive service account's "Shared with Me" directory to upload the
                                    analysis graphs from this pipeline run to.
        :type analysis_graphs_dir: str
        """
        self.drive_credentials_file_url = drive_credentials_file_url
        self.production_upload_path = production_upload_path
        self.messages_upload_path = messages_upload_path
        self.individuals_upload_path = individuals_upload_path
        self.analysis_graphs_dir = analysis_graphs_dir

        self.validate()

    @classmethod
    def from_configuration_dict(cls, configuration_dict):
        drive_credentials_file_url = configuration_dict["DriveCredentialsFileURL"]
        production_upload_path = configuration_dict["ProductionUploadPath"]
        messages_upload_path = configuration_dict["MessagesUploadPath"]
        individuals_upload_path = configuration_dict["IndividualsUploadPath"]
        analysis_graphs_dir = configuration_dict["AnalysisGraphsDir"]

        return cls(drive_credentials_file_url, production_upload_path, messages_upload_path,
                   individuals_upload_path, analysis_graphs_dir)

    def validate(self):
        validators.validate_string(self.drive_credentials_file_url, "drive_credentials_file_url")
        assert urlparse(self.drive_credentials_file_url).scheme == "gs", "DriveCredentialsFileURL needs to be a gs " \
                                                                         "URL (i.e. of the form gs://bucket-name/file)"

        validators.validate_string(self.production_upload_path, "production_upload_path")
        validators.validate_string(self.messages_upload_path, "messages_upload_path")
        validators.validate_string(self.individuals_upload_path, "individuals_upload_path")
        validators.validate_string(self.analysis_graphs_dir, "analysis_graphs_dir")
