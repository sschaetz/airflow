#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import pytest

from tests.providers.google.cloud.operators.test_gcs_system_helper import GcsSystemTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context


@pytest.mark.credential_file(GCP_GCS_KEY)
class GoogleCloudStorageExampleDagsTest(GoogleSystemTest):
    helper = GcsSystemTestHelper()

    @provide_gcp_context(GCP_GCS_KEY)
    def setUp(self):
        super().setUp()

        # 1. Create a bucket

        # 2. Create a file to be processed and upload to prefix

        # 3. Create test.py file that processes the file

        self.helper.create_test_file()

    @provide_gcp_context(GCP_GCS_KEY)
    def tearDown(self):
        # 1. Delete test.py file

        # 2. Delete file to be processed

        # 3. Delete bucket

        super().tearDown()

    @provide_gcp_context(GCP_GCS_KEY)
    def test_run_example_dag(self):
        self.run_dag('example_gcs_timespan_file_transform', CLOUD_DAG_FOLDER)
