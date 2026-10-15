#!/usr/bin/env python3
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

"""Fetch all review-relevant information for a GitHub PR in one shot.

Usage: fetch.py <owner/repo> <pr-number>
       fetch.py <github-pr-url>

Examples:
    fetch.py facebookincubator/velox 17495
    fetch.py https://github.com/facebookincubator/velox/pull/17495

Requires only Python's standard library. Set GH_TOKEN or GITHUB_TOKEN for
private repositories or higher API rate limits.
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request


def request_github(path, accept="application/vnd.github+json"):
    headers = {
        "Accept": accept,
        "User-Agent": "velox-pr-review",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    url = f"https://api.github.com/{path}"
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        try:
            message = json.loads(error.read()).get("message", error.reason)
        except (ValueError, UnicodeDecodeError):
            message = error.reason
        sys.exit(
            f"Error: GitHub API request failed: {url}: HTTP {error.code}: {message}"
        )
    except (urllib.error.URLError, TimeoutError) as error:
        sys.exit(f"Error: GitHub API request failed: {url}: {error}")


def fetch_json(path):
    raw = request_github(path)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        sys.exit(f"Error: Invalid JSON from GitHub API: {path}: {error}")


def fetch_pages(path):
    items = []
    page = 1
    while True:
        batch = fetch_json(f"{path}?per_page=100&page={page}")
        items.extend(batch)
        if len(batch) < 100:
            return items
        page += 1


def parse_args():
    if len(sys.argv) == 2:
        match = re.match(r"https?://github\.com/([^/]+/[^/]+)/pull/(\d+)", sys.argv[1])
        if match:
            return match.group(1), match.group(2)
        print(f"Invalid URL: {sys.argv[1]}", file=sys.stderr)
        sys.exit(1)
    elif len(sys.argv) == 3:
        return sys.argv[1], sys.argv[2]
    else:
        print(__doc__.strip(), file=sys.stderr)
        sys.exit(1)


def fetch_pr_metadata(repo, pr):
    path = f"repos/{repo}/pulls/{pr}"
    data = fetch_json(path)
    files = fetch_pages(f"{path}/files")
    return {
        "title": data["title"],
        "body": data["body"] or "",
        "author": {"login": data["user"]["login"]},
        "state": "MERGED" if data["merged"] else data["state"].upper(),
        "headRefName": data["head"]["ref"],
        "baseRefName": data["base"]["ref"],
        "additions": data["additions"],
        "deletions": data["deletions"],
        "files": [{"path": file["filename"]} for file in files],
    }


def fetch_diff(repo, pr):
    return request_github(f"repos/{repo}/pulls/{pr}", "application/vnd.github.diff")


def fetch_issue_comments(repo, pr):
    return fetch_pages(f"repos/{repo}/issues/{pr}/comments")


def fetch_review_comments(repo, pr):
    return fetch_pages(f"repos/{repo}/pulls/{pr}/comments")


def fetch_reviews(repo, pr):
    return fetch_pages(f"repos/{repo}/pulls/{pr}/reviews")


def print_section(title, content):
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print(f"{'=' * 60}\n")
    print(content)


def main():
    repo, pr = parse_args()

    metadata = fetch_pr_metadata(repo, pr)
    diff = fetch_diff(repo, pr)
    comments = fetch_issue_comments(repo, pr)
    reviews = fetch_reviews(repo, pr)
    inline = fetch_review_comments(repo, pr)

    print_section(
        f"PR #{pr}: {metadata['title']}",
        f"Author: {metadata['author']['login']}\n"
        f"State: {metadata['state']}\n"
        f"Branch: {metadata['headRefName']} -> {metadata['baseRefName']}\n"
        f"+{metadata['additions']} -{metadata['deletions']}\n"
        f"Files: {', '.join(f['path'] for f in metadata['files'])}\n"
        f"\n{metadata['body']}",
    )

    if diff:
        print_section("Diff", diff)

    skip = {"netlify[bot]", "github-actions[bot]"}

    visible = [c for c in comments if c["user"]["login"] not in skip]
    if visible:
        lines = []
        for c in visible:
            lines.append(f"--- {c['user']['login']} ({c['created_at']}) ---")
            lines.append(c["body"])
            lines.append("")
        print_section("Comments", "\n".join(lines))

    visible_reviews = [r for r in reviews if r.get("body")]
    if visible_reviews:
        lines = []
        for r in visible_reviews:
            lines.append(
                f"--- {r['user']['login']} ({r['state']}, {r['submitted_at']}) ---"
            )
            lines.append(r["body"])
            lines.append("")
        print_section("Reviews", "\n".join(lines))

    if inline:
        lines = []
        for c in inline:
            line = c.get("line") or c.get("original_line") or "?"
            loc = f"{c['path']}:{line}"
            lines.append(f"--- {c['user']['login']} on {loc} ({c['created_at']}) ---")
            lines.append(c["body"])
            lines.append("")
        print_section("Inline Comments", "\n".join(lines))


if __name__ == "__main__":
    main()
