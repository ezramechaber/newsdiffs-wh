#!/usr/bin/python

from datetime import datetime
import errno
from frontend import models
import httplib
import logging
import os
import subprocess
import sys
import time
import traceback
import urllib2

import diff_match_patch

import parsers
from parsers.baseparser import canonicalize, formatter, logger

from django.core.management.base import BaseCommand
from optparse import make_option

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            '--update',
            action='store_true',
            default=False,
            help='DEPRECATED; this is the default')
        parser.add_argument(
            '--all',
            action='store_true',
            default=False,
            help='Update _all_ stored articles')
        parser.add_argument(
            '--fakeadiff',
            action='store_true',
            default=False,
            help='Add a fake change to end of each article')

    help = '''Scrape websites.

By default, scan front pages for new articles, and scan
existing and new articles to archive their current contents.

Articles that haven't changed in a while are skipped if we've
scanned them recently, unless --all is passed.
'''.strip()

    def handle(self, *args, **options):
        # TODO(awong): Use some sort of sane logging libray.
        ch = logging.FileHandler('/tmp/newsdiffs_logging', mode='w')
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        ch = logging.FileHandler('/tmp/newsdiffs_logging_errs', mode='a')
        ch.setLevel(logging.WARNING)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        update_articles()
        update_versions(options['all'], options['fakeadiff'])

# Begin utility functions

def canonicalize_url(url):
    return url.split('?')[0].split('#')[0].strip()

def get_all_article_urls():
    ans = set()
    for parser in parsers.parsers:
        logger.info('Looking up %s' % parser.domains)
        urls = parser.feed_urls()
        ans = ans.union(map(canonicalize_url, urls))
    return ans

CHARSET_LIST = """EUC-JP GB2312 EUC-KR Big5 SHIFT_JIS windows-1252
IBM855
IBM866
ISO-8859-2
ISO-8859-5
ISO-8859-7
KOI8-R
MacCyrillic
TIS-620
windows-1250
windows-1251
windows-1253
windows-1255""".split()
def is_boring(old, new):
    oldu = canonicalize(old.decode('utf8'))
    newu = canonicalize(new.decode('utf8'))

    def extra_canonical(s):
        """Ignore changes in whitespace or the date line"""
        nondate_portion = s.split('\n', 1)[1]
        return nondate_portion.split()

    if extra_canonical(oldu) == extra_canonical(newu):
        return True

    for charset in CHARSET_LIST:
        try:
            if oldu.encode(charset) == new:
                logger.debug('Boring!')
                return True
        except UnicodeEncodeError:
            pass
    return False

def get_diff_info(old, new):
    dmp = diff_match_patch.diff_match_patch()
    dmp.Diff_Timeout = 3 # seconds; default of 1 is too little
    diff = dmp.diff_main(old, new)
    dmp.diff_cleanupSemantic(diff)
    chars_added   = sum(len(text) for (sign, text) in diff if sign == 1)
    chars_removed = sum(len(text) for (sign, text) in diff if sign == -1)
    return dict(chars_added=chars_added, chars_removed=chars_removed)


def load_article(url):
    try:
        parser = parsers.get_parser(url)
    except KeyError:
        logger.info('Unable to parse domain, skipping')
        return
    try:
        parsed_article = parser(url)
    except (AttributeError, urllib2.HTTPError, httplib.HTTPException), e:
        if isinstance(e, urllib2.HTTPError) and e.msg == 'Gone':
            return
        logger.error('Exception when parsing %s', url)
        logger.error(traceback.format_exc())
        logger.error('Continuing')
        return
    if not parsed_article.real_article:
        return
    return parsed_article

#Return whether it changed
def update_article(article, fakeadiff=False):
    parsed_article = load_article(article.url)
    if parsed_article is None:
        return
    to_store = unicode(parsed_article).encode('utf8')
    t = datetime.now()
    if fakeadiff:
        to_store = '~~ FAKE DIFF ~~\n%s ~~ %s' % (to_store, t)
    logger.debug('Article parsed; trying to store')
    textblob = models.TextBlob.create_or_get(to_store)

    boring = False
    # TODO(awong): Find previous version and store?
    diff_info = None
    prev = models.Version.objects.filter(article=article).order_by('-date').first()
    if prev:
        boring = is_boring(prev.text.blob, to_store)

    if not boring:
        textblob.save()
        v_row = models.Version(text = textblob,
                               boring=boring,
                               title=parsed_article.title,
                               byline=parsed_article.byline,
                               date=t,
                               article=article,
                               )
        if prev is not None:
            v_row.diff_info = get_diff_info(prev.text.blob, to_store)
        v_row.save()
        article.last_update = t
        article.save()

def update_articles():
    logger.info('Starting scraper; looking for new URLs')
    all_urls = get_all_article_urls()
    logger.info('Got all %s urls; storing to database' % len(all_urls))
    for i, url in enumerate(all_urls):
        logger.debug('Woo: %d/%d is %s' % (i+1, len(all_urls), url))
        if len(url) > 255:  #Icky hack, but otherwise they're truncated in DB.
            continue
        if not models.Article.objects.filter(url=url).count():
            logger.debug('Adding!')
            models.Article(url=url).save()
    logger.info('Done storing to database')

def get_update_delay(minutes_since_update):
    days_since_update = minutes_since_update // (24 * 60)
    if minutes_since_update < 60*3:
        return 15
    elif days_since_update < 1:
        return 60
    elif days_since_update < 7:
        return 180
    elif days_since_update < 30:
        return 60*24*3
    elif days_since_update < 360:
        return 60*24*30
    else:
        return 60*24*365*1e5  #ignore old articles

def update_versions(do_all=False, fakeadiff=False):
    logger.info('Looking for articles to check')
    # TODO(awong): This loads all articles at once instead of using a cusor. Why?
    articles = list(models.Article.objects.all())
    total_articles = len(articles)

    update_priority = lambda x: x.minutes_since_check() * 1. / get_update_delay(x.minutes_since_update())
    articles = sorted([a for a in articles if update_priority(a) > 1 or do_all],
                      key=update_priority, reverse=True)

    logger.info('Checking %s of %s articles', len(articles), total_articles)

    for i, article in enumerate(articles):
        logger.debug('Woo: %s %s %s (%s/%s)',
                     article.minutes_since_update(),
                     article.minutes_since_check(),
                     update_priority(article), i+1, len(articles))
        delay = get_update_delay(article.minutes_since_update())
        if article.minutes_since_check() < delay and not do_all:
            continue
        logger.info('Considering %s', article.url)

        article.last_check = datetime.now()
        try:
            update_article(article, fakeadiff)
        except Exception, e:
            if isinstance(e, subprocess.CalledProcessError):
                logger.error('CalledProcessError when updating %s', article.url)
                logger.error(repr(e.output))
            else:
                logger.error('Unknown exception when updating %s', article.url)

            logger.error(traceback.format_exc())
        article.save()
    logger.info('Done!')

if __name__ == '__main__':
    print >> sys.stderr, "Try `python website/manage.py scraper`."
