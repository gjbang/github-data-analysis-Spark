# This file will construct the sql statement for the data_gen\timer_spider\timer_spider.py file.
# insert string is based on insert_schema.sql
from datetime import datetime
import json


user_insert_sql = """
        insert into users(                
            id,login,node_id,gravatar_id,type,
            site_admin,name,company,blog,location,
            email,hireable,bio,twitter_username,public_repos,
            public_gists,followers,following,created_at,
            updated_at
        )
        values(
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s
        ) as new
        on DUPLICATE KEY UPDATE
        public_repos= IF(users.updated_at < new.updated_at, new.public_repos, users.public_repos),
        followers= IF(users.updated_at < new.updated_at, new.followers, users.followers),
        following= IF(users.updated_at < new.updated_at, new.following, users.following),
        created_at= IF(users.updated_at < new.updated_at, new.created_at, users.created_at),
        updated_at= IF(users.updated_at < new.updated_at, new.updated_at, users.updated_at);
"""

repo_insert_sql = """
        insert into repos(
            id,node_id,name,full_name,private,
            owner_id,description,fork,created_at,updated_at,
            pushed_at,homepage,size,stargazers_count,watchers_count,
            language,has_issues,has_projects,has_downloads,has_wiki,
            has_pages,forks_count,archived,disabled,open_issues_count,
            license,forks,allow_forking,open_issues,watchers,
            organization_id,topics,default_branch
        )
        values(
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s
        ) as new
        on DUPLICATE KEY UPDATE
        created_at= IF(repos.updated_at < new.updated_at, new.created_at, repos.created_at),
        updated_at= IF(repos.updated_at < new.updated_at, new.updated_at, repos.updated_at),
        pushed_at= IF(repos.updated_at < new.updated_at, new.pushed_at, repos.pushed_at),
        size= IF(repos.updated_at < new.updated_at, new.size, repos.size),
        stargazers_count= IF(repos.updated_at < new.updated_at, new.stargazers_count, repos.stargazers_count),
        watchers_count= IF(repos.updated_at < new.updated_at, new.watchers_count, repos.watchers_count),
        forks_count= IF(repos.updated_at < new.updated_at, new.forks_count, repos.forks_count),
        open_issues_count= IF(repos.updated_at < new.updated_at, new.open_issues_count, repos.open_issues_count),
        forks= IF(repos.updated_at < new.updated_at, new.forks, repos.forks),
        open_issues= IF(repos.updated_at < new.updated_at, new.open_issues, repos.open_issues),
        watchers= IF(repos.updated_at < new.updated_at, new.watchers, repos.watchers);
"""


UTC_FORMAT = "%Y-%m-%dT%H:%M:%S%fZ"

def get_user_insert_sql():
    return user_insert_sql

def get_repo_insert_sql():
    return repo_insert_sql


def cons_user_sql(cjson_list):
    # print("** cons_user_sql " + str(len(cjson_list)))

    value_list = []
    for item in cjson_list:
        # print(item)
        value_list.append((
            item["id"],item["login"],item["node_id"],item["gravatar_id"],item["type"],
            item["site_admin"],item["name"],item["company"],item["blog"],item["location"],
            item["email"],item["hireable"],item["bio"],item["twitter_username"],item["public_repos"],
            item["public_gists"],item["followers"],item["following"],datetime.strptime(item["created_at"],UTC_FORMAT),
            datetime.strptime(item["updated_at"],UTC_FORMAT)
        ))

    # print(len(value_list[0]))


    return value_list

def cons_repo_sql(cjson_list):
    value_list = []
    for item in cjson_list:
        org = item["organization"] if "organization" in item else None
        lic = item["license"] if "license" in item else None
        # cut to 200 max chars with '' as split char
        topics = "'" + "','".join(item["topics"][:20]) + "'" if "topics" in item else None
        # cut description to 250 max chars
        # description = item["description"][:250] if "description" in item else None


        value_list.append((
            item["id"],item["node_id"],item["name"],item["full_name"],item["private"],
            item["owner"]["id"], item["description"], item["fork"],datetime.strptime(item["created_at"],UTC_FORMAT),datetime.strptime(item["updated_at"],UTC_FORMAT),
            datetime.strptime(item["pushed_at"],UTC_FORMAT),item["homepage"],item["size"],item["stargazers_count"],item["watchers_count"],
            item["language"],item["has_issues"],item["has_projects"],item["has_downloads"],item["has_wiki"],
            item["has_pages"],item["forks_count"],item["archived"],item["disabled"],item["open_issues_count"],
            lic["key"] if lic !=None and 'key' in lic else None,
            item["forks"],item["allow_forking"],item["open_issues"],item["watchers"],
            org["id"] if org !=None and 'id' in org else None,
            topics,
            item["default_branch"]
        ))

    # print(len(value_list[0]))

    return value_list




# A simple test cases
if __name__ == "__main__":
    print("hello")
    print(cons_user_sql([{'login': 'ammar-1990', 'id': 94840203, 'node_id': 'U_kgDOBacliw', 'avatar_url': 'https://avatars.githubusercontent.com/u/94840203?v=4', 'gravatar_id': '', 'url': 'https://api.github.com/users/ammar-1990', 'html_url': 'https://github.com/ammar-1990', 'followers_url': 'https://api.github.com/users/ammar-1990/followers', 'following_url': 'https://api.github.com/users/ammar-1990/following{/other_user}', 'gists_url': 'https://api.github.com/users/ammar-1990/gists{/gist_id}', 'starred_url': 'https://api.github.com/users/ammar-1990/starred{/owner}{/repos}', 'subscriptions_url': 'https://api.github.com/users/ammar-1990/subscriptions', 'organizations_url': 'https://api.github.com/users/ammar-1990/orgs', 'repos_url': 'https://api.github.com/users/ammar-1990/repos', 'events_url': 'https://api.github.com/users/ammar-1990/events{/privacy}', 'received_events_url': 'https://api.github.com/users/ammar-1990/received_events', 'type': 'users', 'site_admin': False, 'name': None, 'company': None, 'blog': '', 'location': None, 'email': None, 'hireable': None, 'bio': None, 'twitter_username': None, 'public_repos': 23, 'public_gists': 0, 'followers': 0, 'following': 0, 'created_at': '2021-11-22T11:42:56Z', 'updated_at': '2023-04-12T14:26:29Z'}]))
    print(cons_repo_sql([{'id': 558903906, 'node_id': 'R_kgDOIVAyYg', 'name': 'minecraft-dark-souls-edition', 'full_name': 'Steveplays28/minecraft-dark-souls-edition', 'private': False, 'owner': {'login': 'Steveplays28', 'id': 62797992, 'node_id': 'MDQ6VXNlcjYyNzk3OTky', 'avatar_url': 'https://avatars.githubusercontent.com/u/62797992?v=4', 'gravatar_id': '', 'url': 'https://api.github.com/users/Steveplays28', 'html_url': 'https://github.com/Steveplays28', 'followers_url': 'https://api.github.com/users/Steveplays28/followers', 'following_url': 'https://api.github.com/users/Steveplays28/following{/other_user}', 'gists_url': 'https://api.github.com/users/Steveplays28/gists{/gist_id}', 'starred_url': 'https://api.github.com/users/Steveplays28/starred{/owner}{/repos}', 'subscriptions_url': 'https://api.github.com/users/Steveplays28/subscriptions', 'organizations_url': 'https://api.github.com/users/Steveplays28/orgs', 'repos_url': 'https://api.github.com/users/Steveplays28/repos', 'events_url': 'https://api.github.com/users/Steveplays28/events{/privacy}', 'received_events_url': 'https://api.github.com/users/Steveplays28/received_events', 'type': 'users', 'site_admin': False}, 'html_url': 'https://github.com/Steveplays28/minecraft-dark-souls-edition', 'description': 'Minecraft: Dark Souls Edition modpack', 'fork': False, 'url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition', 'forks_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/forks', 'keys_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/keys{/key_id}', 'collaborators_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/collaborators{/collaborator}', 'teams_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/teams', 'hooks_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/hooks', 'issue_events_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/issues/events{/number}', 'events_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/events', 'assignees_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/assignees{/users}', 'branches_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/branches{/branch}', 'tags_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/tags', 'blobs_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/git/blobs{/sha}', 'git_tags_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/git/tags{/sha}', 'git_refs_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/git/refs{/sha}', 'trees_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/git/trees{/sha}', 'statuses_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/statuses/{sha}', 'languages_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/languages', 'stargazers_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/stargazers', 'contributors_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/contributors', 'subscribers_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/subscribers', 'subscription_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/subscription', 'commits_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/commits{/sha}', 'git_commits_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/git/commits{/sha}', 'comments_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/comments{/number}', 'issue_comment_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/issues/comments{/number}', 'contents_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/contents/{+path}', 'compare_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/compare/{base}...{head}', 'merges_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/merges', 'archive_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/{archive_format}{/ref}', 'downloads_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/downloads', 'issues_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/issues{/number}', 'pulls_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/pulls{/number}', 'milestones_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/milestones{/number}', 'notifications_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/notifications{?since,all,participating}', 'labels_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/labels{/name}', 'releases_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/releases{/id}', 'deployments_url': 'https://api.github.com/repos/Steveplays28/minecraft-dark-souls-edition/deployments', 'created_at': '2022-10-28T15:00:27Z', 'updated_at': '2023-03-05T18:10:30Z', 'pushed_at': '2023-04-30T18:23:34Z', 'git_url': 'git://github.com/Steveplays28/minecraft-dark-souls-edition.git', 'ssh_url': 'git@github.com:Steveplays28/minecraft-dark-souls-edition.git', 'clone_url': 'https://github.com/Steveplays28/minecraft-dark-souls-edition.git', 'svn_url': 'https://github.com/Steveplays28/minecraft-dark-souls-edition', 'homepage': 'https://discord.gg/KbWxgGg', 'size': 5309, 'stargazers_count': 0, 'watchers_count': 0, 'language': 'CSS', 'has_issues': True, 'has_projects': False, 'has_downloads': True, 'has_wiki': True, 'has_pages': False, 'has_discussions': False, 'forks_count': 0, 'mirror_url': None, 'archived': False, 'disabled': False, 'open_issues_count': 34, 'license': None, 'allow_forking': True, 'is_template': False, 'web_commit_signoff_required': False, 'topics': ['minecraft', 'packwiz', 'quiltmc'], 'visibility': 'public', 'forks': 0, 'open_issues': 34, 'watchers': 0, 'default_branch': 'main', 'permissions': {'admin': False, 'maintain': False, 'push': False, 'triage': False, 'pull': True}, 'temp_clone_token': '', 'network_count': 0, 'subscribers_count': 1}]))