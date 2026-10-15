# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import json
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

from scripts.review import fetch


class ReviewFetchTest(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict("os.environ", {}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.urlopen = patch("urllib.request.urlopen").start()
        self.addCleanup(patch.stopall)

    def response(self, data):
        response = MagicMock()
        text = data if isinstance(data, str) else json.dumps(data)
        response.__enter__.return_value.read.return_value = text.encode("utf-8")
        return response

    def test_metadata(self):
        self.urlopen.side_effect = [
            self.response(
                {
                    "title": "Test PR",
                    "body": "Description",
                    "user": {"login": "contributor"},
                    "state": "closed",
                    "merged": True,
                    "head": {"ref": "feature"},
                    "base": {"ref": "main"},
                    "additions": 2,
                    "deletions": 1,
                }
            ),
            self.response([{"filename": "example.cpp"}]),
        ]

        metadata = fetch.fetch_pr_metadata("owner/repo", "123")

        self.assertEqual(
            metadata,
            {
                "title": "Test PR",
                "body": "Description",
                "author": {"login": "contributor"},
                "state": "MERGED",
                "headRefName": "feature",
                "baseRefName": "main",
                "additions": 2,
                "deletions": 1,
                "files": [{"path": "example.cpp"}],
            },
        )
        self.assertEqual(
            self.urlopen.call_args_list[0].args[0].full_url,
            "https://api.github.com/repos/owner/repo/pulls/123",
        )

    def test_paginated_lists(self):
        for function, endpoint in [
            (fetch.fetch_issue_comments, "issues/123/comments"),
            (fetch.fetch_review_comments, "pulls/123/comments"),
            (fetch.fetch_reviews, "pulls/123/reviews"),
        ]:
            with self.subTest(endpoint=endpoint):
                self.urlopen.reset_mock()
                first_page = [{"id": index} for index in range(100)]
                last_page = [{"id": 100}]
                self.urlopen.side_effect = [
                    self.response(first_page),
                    self.response(last_page),
                ]

                self.assertEqual(function("owner/repo", "123"), first_page + last_page)
                self.assertEqual(
                    [call.args[0].full_url for call in self.urlopen.call_args_list],
                    [
                        f"https://api.github.com/repos/owner/repo/{endpoint}"
                        f"?per_page=100&page={page}"
                        for page in (1, 2)
                    ],
                )

    def test_diff_and_authentication(self):
        for environment, token in [
            ({}, None),
            ({"GH_TOKEN": "test-gh-token"}, "test-gh-token"),
            ({"GITHUB_TOKEN": "test-github-token"}, "test-github-token"),
            (
                {"GH_TOKEN": "test-gh-token", "GITHUB_TOKEN": "test-github-token"},
                "test-gh-token",
            ),
        ]:
            with self.subTest(environment=environment):
                with patch.dict("os.environ", environment, clear=True):
                    self.urlopen.return_value = self.response("diff --git caf\u00e9\n")
                    self.assertEqual(
                        fetch.fetch_diff("owner/repo", "123"), "diff --git caf\u00e9\n"
                    )
                request = self.urlopen.call_args.args[0]
                self.assertEqual(
                    request.get_header("Accept"), "application/vnd.github.diff"
                )
                self.assertEqual(
                    request.get_header("Authorization"),
                    f"Bearer {token}" if token else None,
                )
                self.assertEqual(self.urlopen.call_args.kwargs["timeout"], 30)

    def test_request_failure_exits(self):
        for error, message in [
            (
                HTTPError(
                    "https://api.github.com/test",
                    403,
                    "Forbidden",
                    {},
                    io.BytesIO(b'{"message": "API rate limit exceeded"}'),
                ),
                "API rate limit exceeded",
            ),
            (
                HTTPError(
                    "https://api.github.com/test",
                    502,
                    "Bad Gateway",
                    {},
                    io.BytesIO(b"<html>Bad Gateway</html>"),
                ),
                "Bad Gateway",
            ),
            (URLError("connection refused"), "connection refused"),
            (TimeoutError("timed out"), "timed out"),
        ]:
            with self.subTest(message=message):
                self.urlopen.side_effect = error
                with self.assertRaisesRegex(SystemExit, message):
                    fetch.fetch_diff("owner/repo", "123")

    def test_invalid_json_exits(self):
        self.urlopen.return_value = self.response("not json")
        with self.assertRaisesRegex(SystemExit, "Invalid JSON"):
            fetch.fetch_reviews("owner/repo", "123")

    def test_argument_forms(self):
        for arguments in [
            ["fetch.py", "owner/repo", "123"],
            ["fetch.py", "https://github.com/owner/repo/pull/123"],
        ]:
            with self.subTest(arguments=arguments):
                with patch("sys.argv", arguments):
                    self.assertEqual(fetch.parse_args(), ("owner/repo", "123"))

    def test_main_output(self):
        responses = [
            {
                "title": "Test PR",
                "body": None,
                "user": {"login": "contributor"},
                "state": "open",
                "merged": False,
                "head": {"ref": "feature"},
                "base": {"ref": "main"},
                "additions": 2,
                "deletions": 1,
            },
            [{"filename": "example.cpp"}],
            "diff --git example.cpp\n",
            [
                {
                    "user": {"login": "reviewer"},
                    "created_at": "2026-01-01",
                    "body": "General feedback",
                },
                {"user": {"login": "github-actions[bot]"}},
            ],
            [
                {
                    "user": {"login": "reviewer"},
                    "state": "COMMENTED",
                    "submitted_at": "2026-01-01",
                    "body": "Review feedback",
                },
                {"body": ""},
            ],
            [
                {
                    "user": {"login": "reviewer"},
                    "path": "example.cpp",
                    "line": None,
                    "original_line": 42,
                    "created_at": "2026-01-01",
                    "body": "Inline feedback",
                }
            ],
        ]
        self.urlopen.side_effect = [self.response(data) for data in responses]
        with (
            patch("sys.argv", ["fetch.py", "owner/repo", "123"]),
            patch("sys.stdout", new_callable=io.StringIO) as output,
        ):
            fetch.main()
        text = output.getvalue()
        for expected in [
            "PR #123: Test PR",
            "Author: contributor",
            "State: OPEN",
            "Branch: feature -> main",
            "+2 -1",
            "Files: example.cpp",
            "diff --git example.cpp",
            "Comments",
            "General feedback",
            "Reviews",
            "Review feedback",
            "Inline Comments",
            "reviewer on example.cpp:42",
            "Inline feedback",
        ]:
            self.assertIn(expected, text)
        self.assertNotIn("github-actions[bot]", text)
        self.assertNotIn("None", text)

    def test_main_does_not_print_partial_context(self):
        with (
            patch("sys.argv", ["fetch.py", "owner/repo", "123"]),
            patch.object(fetch, "fetch_pr_metadata", return_value={"title": "Test"}),
            patch.object(fetch, "fetch_diff", side_effect=SystemExit("Fetch failed")),
            patch("sys.stdout", new_callable=io.StringIO) as output,
        ):
            with self.assertRaisesRegex(SystemExit, "Fetch failed"):
                fetch.main()
        self.assertEqual(output.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
