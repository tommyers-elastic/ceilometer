#
# Copyright 2016 IBM
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
from oslo_utils import strutils
from urllib import parse as urlparse

from ceilometer.publisher import http


class ElasticsearchPublisher(http.HttpPublisher):
    def __init__(self, conf, parsed_url):
        super(ElasticsearchPublisher, self).__init__(conf, parsed_url)

        http_parsed_url = urlparse.urlparse(self.target)

        # i guess this goes in ceilometer.conf
        params = urlparse.parse_qs(http_parsed_url.query)
        api_key = self._get_param(params, 'api_key', '')

        if not api_key:
            raise ValueError('An API key is required for ElasticsearchPublisher')

        self.HEADERS = {'Content-Type': 'application/json', 'Authorization': 'ApiKey %s' % api_key}

        self.target = urlparse.urlunsplit([
            http_parsed_url.scheme,
            http_parsed_url.netloc,
            http_parsed_url.path,
            urlparse.urlencode(params, doseq=True),
            http_parsed_url.fragment])

        self.poster = (
            self._batch_post if strutils.bool_from_string(self._get_param(
                params, 'batch', True)) else self._individual_post)

    def publish_samples(self, samples):
        for s in samples:
            s = s.as_dict()
            s['@timestamp'] = s['timestamp']
            self._do_post(json.dumps(s))

    def publish_events(self, events):
        self.publish_samples(events)
