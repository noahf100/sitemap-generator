import asyncio
import os
import signal

from pysitemap.base_crawler import Crawler


def crawler(root_url, out_file, out_format='xml', maxtasks=100,
            batch_size=10000, prefix=None, load=False):
    """
    run crowler
    :param root_url: Site root url
    :param out_file: path to the out file
    :param out_format: format of out file [xml, txt]
    :param maxtasks: max count of tasks
    :return:
    """
    loop = asyncio.get_event_loop()

    # TODO (noahfang) - put name in assets file and use here and in base_crawler.py
    # If we do not want to load existing data, delete the data
    if not load:
        try:
            os.remove('seen.sqlite')
            os.remove('todo.sqlite')
        except:
            pass

    c = Crawler(root_url, out_file=out_file, out_format=out_format, maxtasks=maxtasks,
                batch_size=batch_size, prefix=prefix, load=load)
    loop.run_until_complete(c.run())

    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
    except RuntimeError:
        pass
    print('busy:', len(c.busy))
    print('done:', len(c.done), '; ok:', sum(c.done.values()))
