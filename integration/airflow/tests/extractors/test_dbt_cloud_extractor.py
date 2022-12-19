# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import logging
import unittest
# import mock

log = logging.getLogger(__name__)


class TestDbtCloudExtractorE2E(unittest.TestCase):

    def test_extract(self, mock_hook):
        log.info("test_extractor")
        assert 1 + 1 == 2


if __name__ == '__main__':
    unittest.main()
