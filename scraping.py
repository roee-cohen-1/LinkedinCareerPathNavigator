"""
This script scrapes Dice.com for job postings and saves them as HTML files.
It is using `requests` for the scraping itself, and `multiprocessing` libraries for parallelization.

50 Workers are span-up and listen to a common queue for the scraping jobs.
Each job is initiated by a page identifier which is used to scrape Dice.com job exploration page.
Upon scraping the explore page, the job_ids are retrieved and scraped one by one.
As the work on each exploration page is independent on the others, the load can be parallelized.

A job page that were already scraped and save will not be scraped again by another worker.
"""

import json
import time
import warnings
import os
import requests
from requests.exceptions import HTTPError
from multiprocessing import Process, Queue

warnings.filterwarnings("ignore")


class Worker(Process):

    def __init__(self, queue: Queue):
        super().__init__()
        self._queue = queue
        self._proxies = {
            'http': 'YOUR_PROXY_HERE',
            'https': 'YOUR_PROXY_HERE',
        }

    def run(self):
        while True:
            args = self._queue.get()
            if args is None:
                break
            self._process(args)
            print(args, 'Done')

    def _process(self, idx):
        result = self._make_request(
            f'https://job-search-api.svc.dhigroupinc.com/v1/dice/jobs/search?countryCode2=US&radius=5000&radiusUnit=km&page={idx}&pageSize=100&facets=employmentType%7CpostedDate%7CworkFromHomeAvailability%7CworkplaceTypes%7CemployerType%7CeasyApply%7CisRemote&fields=id&culture=en&recommendations=true&interactionId=0&fj=true&includeRemote=true&eid=100465514_1004160904')
        if not result:
            return
        jobs_html = result.text
        for job_id in self._find_all_job_ids(jobs_html):
            if not self._is_job_already_found(job_id):
                job = self._get_job_details(job_id)
                with open(f'dice/{job_id}.html', 'w', encoding='utf-16') as out_file:
                    out_file.write(job)
            print(job_id)

    def _make_request(self, url):
        while True:
            try:
                result = requests.get(url, proxies=self._proxies, verify=False, headers={
                    'X-Api-Key': '1YAt0R9wBg4WfsF9VB2778F5CHLAPMVW3WAZcKd8'
                })
            except Exception as e:
                time.sleep(0.1)
                continue

            # checking for errors -> trying again if needed
            try:
                result.raise_for_status()
            except HTTPError as e:
                if result.status_code not in [429, 451]:
                    return None
                continue

            return result

    def _find_all_job_ids(self, text):
        data = json.loads(text)
        return [item['id'] for item in data['data']]

    def _get_job_details(self, job_id):
        url = 'https://www.dice.com/job-detail/' + job_id
        result = self._make_request(url)
        return result.text if result else ''

    def _is_job_already_found(self, job_id):
        return os.path.exists(f'dice/{job_id}.html')


if __name__ == '__main__':
    jobs = []
    queue = Queue()

    for i in range(50):
        p = Worker(queue)
        jobs.append(p)
        p.start()

    for i in range(1, 100):
        queue.put(i)

    for j in jobs:
        queue.put(None)

    for j in jobs:
        j.join()
