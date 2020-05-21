#!/usr/bin/env python3

import sys

from lxml import html
from urllib.parse import urljoin
import requests


def visit_url(url):
  page = requests.get(url)
  webpage = html.fromstring(page.content)

  urls = webpage.xpath("//a/@href")
  for next_url in urls:
    # Check, whether it's a relative url.
    if not next_url.startswith("http"):
      yield urljoin(url, next_url)
    else:
      yield url


def main():
  for line in sys.stdin:
    url, visited = line.rstrip().split('\t')
    print(f"{url.strip('/')}\t1")

    if visited == '0':
      for next_url in visit_url(url):
        print(f"{next_url.strip('/')}\t0")
    elif visited != '1':
      raise ValueError("Invalid value of record.")


if __name__ == "__main__":
  main()
