import argparse
import sys
import logging
from pysitemap import crawler

def parse_args():
    parser = argparse.ArgumentParser(
        description='Mealswype Web Crawler to collect recipe urls')
    parser.add_argument(
        '--load-data',
        action='store_true',
        help='''If the program should restore.''')
    parser.add_argument(
        '--iocp',
        action='store_true',
        help='''No idea''')
    parser.add_argument(
        '--retry-timeout',
        default=5,
        help='''Duration (in seconds) between retries if a network request ends in failure.''')
    parser.add_argument(
        '--retry-amount',
        default=5,
        help='''Number of retry attempts before giving up.''')

    return parser.parse_args()

if __name__ == '__main__':
    ARGS = parse_args()

    if ARGS.iocp:
        from asyncio import events, windows_events
        sys.argv.remove('--iocp')
        logging.info('using iocp')
        el = windows_events.ProactorEventLoop()
        events.set_event_loop(el)

    load = ARGS.load_data

    # root_url = sys.argv[1]
    #root_url = 'https://www.hellofresh.com/recipes'
    root_url = 'https://www.allrecipes.com/'
    crawler(root_url, out_file='sitemap.xml', batch_size=10000, prefix='https://www.allrecipes.com/recipe/', load=load)