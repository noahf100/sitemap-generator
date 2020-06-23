import aiohttp
import asyncio
import logging
import re
import urllib.parse
import validators

from sqlitedict import SqliteDict
from pysitemap.format_processors.xml import XMLWriter
from pysitemap.format_processors.text import TextWriter


class Crawler:

    format_processors = {
        'xml': XMLWriter,
        'txt': TextWriter
    }

    def __init__(self, rooturl, out_file, out_format='xml', maxtasks=100,
                 todo_queue_backend=set, done_backend=dict, batch_size=10000,
                 prefix=None, load=False):
        """
        Crawler constructor
        :param rooturl: root url of site
        :type rooturl: str

        :param out_file: file to save sitemap result
        :type out_file: str

        :param out_format: sitemap type [xml | txt]. Default xml
        :type out_format: str

        :param maxtasks: maximum count of tasks. Default 100
        :type maxtasks: int

        :param batch_size: number of websites finished before writing
        :type batch_size: int
        """
        self.rooturl = rooturl

        self.todoSem = asyncio.Semaphore(1)
        self.todo_queue = SqliteDict('./todo.sqlite', autocommit=True)
        
        self.busy = set()
        self.done = done_backend()
        self.sem = asyncio.Semaphore(maxtasks)

        self.seenSem = asyncio.Semaphore(1)
        self.seen = SqliteDict('./seen.sqlite', autocommit=True)
        
        self.maxtasks = maxtasks
        self.load = load

        # For writing files in batches
        self.batch_size = batch_size
        self.fileSem = asyncio.Semaphore(1)
        self.countFinished = 0
        self.fileIndex = 1
        self.out_file = out_file
        self.out_format = out_format

        # Format for url prefix
        self.prefix = self.rooturl
        if prefix is not None:
            self.prefix = prefix

        self.base_filename = self.out_file
        if self.base_filename.endswith(self.out_format):
            dotIndex = self.base_filename.rindex('.')
            self.base_filename = self.base_filename[ : dotIndex] + '{}.{}'
        else:
            self.base_filename += '{}.{}'

        # connector stores cookies between requests and uses connection pool
        self.session = aiohttp.ClientSession()
        self.writer = self.format_processors.get(out_format)

    async def write(self, index):
        """
        Writer that writes files

        :param: index - index of file
        :type: index - int
        """
        await self.writer(self.base_filename.format(index, self.out_format)).write([key for key, value in self.done.items() if value])

    async def run(self):
        """
        Main function to start parsing site
        :return:
        """
        if not self.load:
            await self.addurls([(self.rooturl, '')])
        
        t = asyncio.gather(*[self.maybeAddUrl() for _ in range(self.maxtasks)])
        
        await asyncio.sleep(1)
        while self.busy:
            await asyncio.sleep(1)

        await t
        await self.session.close()
        await self.write(self.fileIndex)
        self.seen.close()
        self.todo_queue.close()

    def canGetNextDictKey(self, dictionary):
        try:
            res = next(dictionary.keys())
            return res, True
        except StopIteration:
            return None, False

    async def maybeAddUrl(self, url=None):
        while True:
            if url is None:
                new_url, wasAble = self.canGetNextDictKey(self.todo_queue)
                if not wasAble:
                    # Sleep
                    await asyncio.sleep(5)
                    # If still busy, try again later
                    if len(self.busy) > 0:
                        continue
                    else:
                        # If not busy, see if any more availible
                        new_url, wasAble = self.canGetNextDictKey(self.todo_queue)
                        # If no more availible, return
                        if not wasAble:
                            return

            deleted = False
            # Acquire todo deletion semaphor
            await self.todoSem.acquire()
            try:
                # Check that we can delete url from todo list
                if new_url not in self.todo_queue:
                    continue
                del self.todo_queue[new_url]
                deleted = True
            finally:
                self.todoSem.release()

            # Maybe we don't need this if statement or the deleted boolean
            if deleted:
                # Create async task
                await self.process(new_url)
                # Add callback into task to release semaphore
                self.sem.release()
                self.busy.remove(new_url)
            
            # If url is specified, only do it once
            if url is not None:
                return
            # Otherwise, add a delay so don't spam the server
            await asyncio.sleep(3)

    async def addurls(self, urls):
        """
        Add urls in queue and run process to parse
        :param urls:
        :return:
        """
        await self.seenSem.acquire()
        try:
            for url, parenturl in urls:
                url = urllib.parse.urljoin(parenturl, url)
                url, frag = urllib.parse.urldefrag(url)
                if (url.startswith(self.rooturl) and
                        validators.url(url)  and
                        url not in self.busy and
                        url not in self.seen and
                        url not in self.todo_queue):
                    self.seen[url] = True
                    self.todo_queue[url] = True
        finally:
            self.seenSem.release()

    async def process(self, url):
        """
        Process single url
        :param url:
        :return:
        """
        print('processing:', url)

        # remove url from basic queue and add it into busy list
        self.busy.add(url)

        try:
            resp = await self.session.get(url)  # await response
        except Exception as exc:
            # on any exception mark url as BAD
            print('...', url, 'has error', repr(str(exc)))
            # Add url without looking at text if it starts with a good prefix
            # We just won't get any other webpages it links to
            if url.startswith(self.prefix):
                self.done[url] = True
        else:
            # only url with status == 200 and content type == 'text/html' parsed
            if (resp.status == 200 and
                    ('text/html' in resp.headers.get('content-type'))):
                retryCount = 0
                data = ''
                while retryCount < 5:
                    try:
                        data = (await resp.read()).decode('utf-8', 'replace')
                        break
                    except:
                        retryCount += 1
                
                urls = re.findall(r'(?i)href=["\']?([^\s"\'<>]+)', data)
                await self.addurls([(u, url) for u in urls])
            # even if we have no exception, we can mark url as good
            resp.close()
            # Prep url for write if it begins with prefix
            if url.startswith(self.prefix):
                self.done[url] = True
                self.countFinished += 1
        await self.maybeWriteFile()
        logging.info(len(self.done))

    async def maybeWriteFile(self):
        # If number of finished tasks is the same as the batch_size
        if self.countFinished == self.batch_size:
            # Acquire semaphore
            await self.fileSem.acquire()
            try:
                # Write file
                await self.write(self.fileIndex)
                # Increment file index to avoid overwriting
                self.fileIndex += 1
                # Reset count of finished
                self.countFinished = 0
                # Reset done 
                self.done.clear()
            finally:
                self.fileSem.release()
