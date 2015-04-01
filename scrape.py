import scrapekit
from urlparse import urljoin
import string
from lxml.cssselect import CSSSelector
import dataset
import re
import json
import os.path
from collections import defaultdict
from pprint import pprint
from copy import deepcopy

PITCHFORK_ARTIST_URL_PAT = re.compile('^.*pitchfork.com/artists/(?P<pitchfork_slug>(?P<pitchfork_id>\d+)-[^\/]+)')
config = {
  'threads': 15,
  'cache_policy': 'http',
  'data_path': 'data'
}

SQLITE_URL = 'sqlite:///{}'.format(os.path.join(config['data_path'],'pitchfork_scrape.sqlite'))
import os
try:
    os.makedirs(config['data_path'])
except OSError: pass
db = dataset.connect(SQLITE_URL)

scraper = scrapekit.Scraper('pitchfork-artists', config=config)
UNFAMILIAR = defaultdict(list)

@scraper.task
def artist_indexes():
    letters = list(string.lowercase)
    letters.append('other')
    letters = ['a'] # temporary
    for letter in letters:
        yield u'http://pitchfork.com/artists/by/{}/'.format(letter)

@scraper.task
def index_artists(url):
    doc = scraper.get(url).html()
    div_ary = doc.xpath(CSSSelector('#artist-list').path)
    if len(div_ary) == 0:
        scraper.log(u"Did not find an #artist-list for {}".format(url))
    else:
        for i,a in enumerate(div_ary[0].findall('.//a')):
            this_url = a.attrib.get('href')
            if this_url:
                yield urljoin(url,this_url)
            else:
                scraper.log(u"No href for link {} on {}".format(i,url))
    next_link_ary = doc.xpath(CSSSelector('#main .pagination .next').path)
    if next_link_ary:
        next_url = urljoin(url,next_link_ary[0].attrib['href'])
        for artist_url in index_artists(next_url):
            yield artist_url

def reviews_and_tracks(base_url, kind, container):
    coverage = []
    for a in container.findall('.//a'):
        cvrg_url = urljoin(base_url, a.attrib['href'])
        this_cvrg = {'kind': kind, 'url': cvrg_url, 'author': None}
        this_cvrg['image'] = a.find('.//img').attrib['src']
        h1 = a.find('.//h1').text # artist for reviews and tracks, gallery title for photo galleries
        h2 = None
        if a.find('.//h2') is not None:
            album = a.find('.//h2').text
            title = u'{}: {}'.format(h1, album)
        else:
            title = h1 # photo galleries don't have an h2
        this_cvrg['title'] = title
        if a.find('.//h3') is not None:
            this_cvrg['author'] = a.find('.//h3').text
        coverage.append(this_cvrg)
    return coverage

def elaborate_news_and_features(base_url, kind, container):
    coverage = []
    for li in container.findall('.//li'):
        a = li.find('.//a')
        title = a.text
        cvrg_url = urljoin(base_url, a.attrib['href'])
        this_cvrg = {'kind': kind, 'url': cvrg_url, 'title': title}
        coverage.append(this_cvrg)
    return coverage

COVERAGE_PROCESSORS = {
    'Album Reviews': reviews_and_tracks,
    'Tracks': reviews_and_tracks,
    'Forkcast': reviews_and_tracks,
    'Photo Galleries': reviews_and_tracks,
    'Features': elaborate_news_and_features,
    'News': elaborate_news_and_features,
    'Pitchfork.tv': reviews_and_tracks,
    'The Pitch': elaborate_news_and_features,
}

def make_group_dict(doc):
    d = {}
    for group in doc.xpath(CSSSelector('.search-group').path):
        kind = group.find('h1').text.strip()
        d[kind] = group
    return d

@scraper.task
def extract_artist_data(item_url):
    print item_url
    #eg http://pitchfork.com/artists/30707-todolo/
    match = PITCHFORK_ARTIST_URL_PAT.match(item_url)
    d = match.groupdict()
    coverage = []
    d['pitchfork_id'] = int(d['pitchfork_id'])
    d['artist_url'] = item_url
    d['coverage'] = coverage
    d['coverage_types'] = {}
    doc = scraper.get(item_url).html()
    h1_ary = doc.xpath(CSSSelector("#main .object-detail h1").path)
    if h1_ary:
        d['name'] = h1_ary[0].text
    group_dict = make_group_dict(doc)
    for kind,group in group_dict.items():
        try:
            processor = COVERAGE_PROCESSORS[kind]
            this_cvrg = processor(item_url, kind, group)
            coverage.extend(this_cvrg)
        except KeyError:
            if kind not in UNFAMILIAR:
                scraper.log.warning(u"Unfamiliar coverage type {}".format(kind))
            UNFAMILIAR[kind].append(item_url)

        d['coverage_types'][kind] = len(this_cvrg)
    print d['pitchfork_slug']
    return d

@scraper.task
def save_to_db(data):
    print "save to DB"
    d = deepcopy(data)
    artist_table = db['artist']
    coverage = d.pop('coverage')
    types = d.pop('coverage_types')
    for k,v in types.items():
        d[k] = v
    artist_table.upsert(d,['pitchfork_id'])

    cov_table = db['coverage']
    cov_table.delete(artist_id=d['pitchfork_id'])
    for c in coverage:
        c['artist_id'] = d['pitchfork_id']
        cov_table.insert(c)

    return data # unchanged original


@scraper.task
def write_artist_json(d):
    try:
        fn = os.path.join(config['data_path'],u'{pitchfork_slug}.json'.format(**d))
        with open(fn,'wb') as f:
            json.dump(d,f,indent=2)
    except Exception, e:
        scraper.log.error(e)

def main():
    pipeline = artist_indexes | index_artists | extract_artist_data > save_to_db # > write_artist_json
    pipeline.run()
    print
    pprint(UNFAMILIAR)
    print

if __name__ == '__main__':
    main()