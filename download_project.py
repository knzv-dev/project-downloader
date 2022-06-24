#!python

from dataclasses import dataclass
import multiprocessing
import sys

import requests
import reactivex as rx
from reactivex.scheduler import ThreadPoolScheduler
from reactivex import operators as ops
from git import Repo

REPOSITORIES_LIST_URL_TEMPLATE = "http://git.moscow.alfaintra.net/rest/api/1.0/projects/{0}/repos"

# 1. receive git project name
if sys.argv[1] == None:
    print('Give me project name')
    exit(1)

PROJECT_NAME = sys.argv[1]
USERNAME = sys.argv[2]
PASSWORD = sys.argv[3]

# 2. get all git repositories urls for this project
url = REPOSITORIES_LIST_URL_TEMPLATE.format(PROJECT_NAME)
session = requests.Session()
session.auth = USERNAME, PASSWORD
bg_scheduler = ThreadPoolScheduler(multiprocessing.cpu_count() // 2)

@dataclass
class RepoDefinition:
    name: str
    http_link: str
    ssh_link: str

def to_repo_definition(json: str) -> RepoDefinition:
    name = json['name']
    links = json['links']['clone']
    http_link = None
    ssh_link = None
    for link in links:
        protocol = link['name']
        if protocol == 'http':
            http_link = link['href']
        if protocol == 'ssh':
            ssh_link = link['href']
    return RepoDefinition(name, http_link=http_link, ssh_link=ssh_link)

rx.just(PROJECT_NAME).pipe(
    ops.map(lambda x: REPOSITORIES_LIST_URL_TEMPLATE.format(x)),
    ops.map(lambda url: session.get(url)),
    ops.map(lambda response: response.json()),
    ops.map(lambda json: json['values']),
    ops.flat_map(lambda repos: rx.from_iterable(repos).pipe(
        ops.map(lambda repo : rx.just(repo)),
        ops.map(lambda obs: obs.pipe(
            ops.subscribe_on(bg_scheduler),
            ops.map(lambda repo: to_repo_definition(repo)),
            ops.map(lambda x: Repo.clone_from(x.http_link, PROJECT_NAME + '/' + x.name)),
            ops.catch(rx.just(None))
        )),
        ops.merge_all()
    )),
).subscribe(
    lambda result: print(result),
    lambda e: print(e),
    lambda: print("Complete!")
)