import json

def delete_url_properties(data):
    stack = [data]
    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            for key in list(obj.keys()):
                if isinstance(obj[key], str) and ('http://' in obj[key] or 'https://' in obj[key]):
                    del obj[key]
                else:
                    stack.append(obj[key])
        elif isinstance(obj, list):
            stack.extend(obj)


# Example usage
json_data = {
        "id": "28759509919",
        "type": "IssueCommentEvent",
        "actor": {
            "id": 13780613,
            "login": "erlend-aasland",
            "display_login": "erlend-aasland",
            "gravatar_id": "",
            "url": "https://api.github.com/users/erlend-aasland",
            "avatar_url": "https://avatars.githubusercontent.com/u/13780613?"
        },
        "repo": {
            "id": 13414105,
            "name": "python/peps",
            "url": "https://api.github.com/repos/python/peps"
        },
        "payload": {
            "action": "created",
            "issue": {
                "url": "https://api.github.com/repos/python/peps/issues/3131",
                "repository_url": "https://api.github.com/repos/python/peps",
                "labels_url": "https://api.github.com/repos/python/peps/issues/3131/labels{/name}",
                "comments_url": "https://api.github.com/repos/python/peps/issues/3131/comments",
                "events_url": "https://api.github.com/repos/python/peps/issues/3131/events",
                "html_url": "https://github.com/python/peps/pull/3131",
                "id": 1690039972,
                "node_id": "PR_kwDOAMyu2c5PdhUz",
                "number": 3131,
                "title": "PEP 443: Fix collections.abc example",
                "user": {
                    "login": "erlend-aasland",
                    "id": 13780613,
                    "node_id": "MDQ6VXNlcjEzNzgwNjEz",
                    "avatar_url": "https://avatars.githubusercontent.com/u/13780613?v=4",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/erlend-aasland",
                    "html_url": "https://github.com/erlend-aasland",
                    "followers_url": "https://api.github.com/users/erlend-aasland/followers",
                    "following_url": "https://api.github.com/users/erlend-aasland/following{/other_user}",
                    "gists_url": "https://api.github.com/users/erlend-aasland/gists{/gist_id}",
                    "starred_url": "https://api.github.com/users/erlend-aasland/starred{/owner}{/repo}",
                    "subscriptions_url": "https://api.github.com/users/erlend-aasland/subscriptions",
                    "organizations_url": "https://api.github.com/users/erlend-aasland/orgs",
                    "repos_url": "https://api.github.com/users/erlend-aasland/repos",
                    "events_url": "https://api.github.com/users/erlend-aasland/events{/privacy}",
                    "received_events_url": "https://api.github.com/users/erlend-aasland/received_events",
                    "type": "User",
                    "site_admin": False
                },
                "labels": [],
                "state": "open",
                "locked": False,
                "assignee": None,
                "assignees": [],
                "milestone": None,
                "comments": 4,
                "created_at": "2023-04-30T19:00:01Z",
                "updated_at": "2023-04-30T20:39:25Z",
                "closed_at": None,
                "author_association": "CONTRIBUTOR",
                "active_lock_reason": None,
                "draft": False,
                "pull_request": {
                    "url": "https://api.github.com/repos/python/peps/pulls/3131",
                    "html_url": "https://github.com/python/peps/pull/3131",
                    "diff_url": "https://github.com/python/peps/pull/3131.diff",
                    "patch_url": "https://github.com/python/peps/pull/3131.patch",
                    "merged_at": None
                },
                "body": "\r\n\r\n<!-- readthedocs-preview pep-previews start -->\r\n----\n:books: Documentation preview :books:: https://pep-previews--3131.org.readthedocs.build/\n\r\n<!-- readthedocs-preview pep-previews end -->",
                "reactions": {
                    "url": "https://api.github.com/repos/python/peps/issues/3131/reactions",
                    "total_count": 0,
                    "+1": 0,
                    "-1": 0,
                    "laugh": 0,
                    "hooray": 0,
                    "confused": 0,
                    "heart": 0,
                    "rocket": 0,
                    "eyes": 0
                },
                "timeline_url": "https://api.github.com/repos/python/peps/issues/3131/timeline",
                "performed_via_github_app": None,
                "state_reason": None
            },
            "comment": {
                "url": "https://api.github.com/repos/python/peps/issues/comments/1529134372",
                "html_url": "https://github.com/python/peps/pull/3131#issuecomment-1529134372",
                "issue_url": "https://api.github.com/repos/python/peps/issues/3131",
                "id": 1529134372,
                "node_id": "IC_kwDOAMyu2c5bJL0k",
                "user": {
                    "login": "erlend-aasland",
                    "id": 13780613,
                    "node_id": "MDQ6VXNlcjEzNzgwNjEz",
                    "avatar_url": "https://avatars.githubusercontent.com/u/13780613?v=4",
                    "gravatar_id": "",
                    "url": "https://api.github.com/users/erlend-aasland",
                    "html_url": "https://github.com/erlend-aasland",
                    "followers_url": "https://api.github.com/users/erlend-aasland/followers",
                    "following_url": "https://api.github.com/users/erlend-aasland/following{/other_user}",
                    "gists_url": "https://api.github.com/users/erlend-aasland/gists{/gist_id}",
                    "starred_url": "https://api.github.com/users/erlend-aasland/starred{/owner}{/repo}",
                    "subscriptions_url": "https://api.github.com/users/erlend-aasland/subscriptions",
                    "organizations_url": "https://api.github.com/users/erlend-aasland/orgs",
                    "repos_url": "https://api.github.com/users/erlend-aasland/repos",
                    "events_url": "https://api.github.com/users/erlend-aasland/events{/privacy}",
                    "received_events_url": "https://api.github.com/users/erlend-aasland/received_events",
                    "type": "User",
                    "site_admin": False
                },
                "created_at": "2023-04-30T20:39:25Z",
                "updated_at": "2023-04-30T20:39:25Z",
                "author_association": "CONTRIBUTOR",
                "body": "Good point. I'll leave it for Łukasz to decide.",
                "reactions": {
                    "url": "https://api.github.com/repos/python/peps/issues/comments/1529134372/reactions",
                    "total_count": 0,
                    "+1": 0,
                    "-1": 0,
                    "laugh": 0,
                    "hooray": 0,
                    "confused": 0,
                    "heart": 0,
                    "rocket": 0,
                    "eyes": 0
                },
                "performed_via_github_app": None
            }
        },
        "public": True,
        "created_at": "2023-04-30T20:39:25Z",
        "org": {
            "id": 1525981,
            "login": "python",
            "gravatar_id": "",
            "url": "https://api.github.com/orgs/python",
            "avatar_url": "https://avatars.githubusercontent.com/u/1525981?"
        }
    }

print('Before deletion:')
print(json.dumps(json_data, indent=2))

delete_url_properties(json_data)

print('\nAfter deletion:')
print(json.dumps(json_data, indent=2))
