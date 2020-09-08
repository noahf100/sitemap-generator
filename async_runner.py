import argparse
import asyncio
import signal
import sys
from async_crawler import AsyncCrawler

DEFAULT_ROOT_URL = 'https://www.hellofresh.com/'#'https://www.allrecipes.com/'
DEFAULT_PREFIX = 'https://www.hellofresh.com/recipes/'#'https://www.allrecipes.com/recipe/'

def parse_args():
    parser = argparse.ArgumentParser(
        description='Mealswype Web Crawler to collect recipe urls')
    parser.add_argument(
        '--load-data',
        action='store_true',
        help='''If the program should restore.''')
    parser.add_argument(
        '--retry-timeout',
        default=5,
        help='''Duration (in seconds) between retries if a network request ends in failure.''')
    parser.add_argument(
        '--retry-amount',
        default=5,
        help='''Number of retry attempts before giving up.''')
    parser.add_argument(
        '--base-url',
        default=DEFAULT_ROOT_URL,
        help='''Url to start searching from.''')
    parser.add_argument(
        '---url-prefix',
        default=DEFAULT_PREFIX,
        help='''Prefix to accept.''')
    parser.add_argument(
        '---batch-size',
        default=10000,
        help='''Number of entries to hold in memory before writing file''')

    return parser.parse_args()

if __name__ == '__main__':
    ARGS = parse_args()
    load = ARGS.load_data
    root_url = ARGS.base_url
    prefix = ARGS.url_prefix
    batch_size = ARGS.batch_size

    loop = asyncio.get_event_loop()
    task = AsyncCrawler(
        root_url=root_url,
        out_file='sitemap.xml',
        batch_size=batch_size,
        prefix=prefix,
        load=load
        )
    loop.run_until_complete(task.run())

    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
    except RuntimeError:
        pass

