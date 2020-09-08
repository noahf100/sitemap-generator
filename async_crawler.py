import aiohttp
import asyncio
import logging
import re
import threading
import time
import urllib.parse
import validators

from pysitemap.format_processors.xml import XMLWriter
from pysitemap.format_processors.text import TextWriter

from disk_queue import DiskQueue


class AsyncCrawler():
    format_processors = {
        'xml': XMLWriter,
        'txt': TextWriter
    }

    def __init__(self, root_url, out_file, load, prefix, batch_size, max_tries=5, delay=3, num_tasks=100, out_format='xml'):
        self.diskQueue = DiskQueue(load)

        self.prefix = prefix
        self.max_tries = max_tries
        self.delay = delay
        self.batch_size = batch_size
        self.out_file = out_file
        self.out_format = out_format
        self.load = load
        self.root_url = root_url

        self.numTasks = num_tasks
        self.fileIndex = 1
        self.writeQueue = []
        self.writeSemaphore = asyncio.Semaphore(1)

        # connector stores cookies between requests and uses connection pool
        self.session = aiohttp.ClientSession()
        self.writer = self.format_processors.get(out_format)

        logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    async def write(self, index):
        await self.writer(self.out_file.format(index, self.out_format)).write([key for key in self.writeQueue])

    async def retry(self, resp, max_tries, delay):
        for _ in range(max_tries):
            try:
                await asyncio.sleep(delay // 1000) 
                return (await resp.read()).decode('utf-8', 'replace')
            except Exception:
                continue

    async def run(self):
        if not self.load:
            self.diskQueue.Push(self.root_url)
        
        t = asyncio.gather(*[self.process() for _ in range(self.numTasks)])
        await t

        await self.session.close()
        await self.write(self.fileIndex)
        self.diskQueue.Close()

    async def process(self):
        while not self.diskQueue.IsDone():
            url = self.diskQueue.Next()

            if url is None:
                await asyncio.sleep(5)
                continue
            
            print(url)

            try:
                resp = await self.session.get(url)  # await response
            except Exception as exc:
                # on any exception mark url as BAD
                logging.info('... {} has error {}'.format(url, repr(str(exc))))
                # Add url without looking at text if it starts with a good prefix
                # We just won't get any other webpages it links to
                if url.startswith(self.prefix):
                    self.writeQueue.append(url)
            else:
                # only url with status == 200 and content type == 'text/html' parsed
                if (resp.status == 200 and (resp.headers.get('content-type') is not None and 'text/html' in resp.headers.get('content-type'))):
                    data = await self.retry(resp, self.max_tries, self.delay)
                    if data:
                        logging.info('Success: {}'.format(url))
                        urls = re.findall(r'(?i)href=["\']?([^\s"\'<>]+)', data)
                        for u in urls:
                            parsed = urllib.parse.urljoin(url, u)
                            final, frag = urllib.parse.urldefrag(parsed)

                            if final.startswith(self.root_url) and validators.url(url):
                                self.diskQueue.Push(final)

                # even if we have no exception, we can mark url as good
                resp.close()

                if url.startswith(self.prefix):
                    self.writeQueue.append(url)
                
                logging.info('To Write: {}'.format(len(self.writeQueue)))
                logging.info('TODO: {}, InProgress {}'.format(len(self.diskQueue.todo), len(self.diskQueue.inProgress)))
            self.diskQueue.Done(url)
            await asyncio.sleep(5)

    async def maybeWriteFile(self):
        # Acquire semaphore
        await self.writeSemaphore.acquire()
        try:
            # If number of finished tasks is the same as the batch_size
            if len(self.writeQueue) == self.batch_size:
                # Write file
                await self.write(self.fileIndex)
                # Increment file index to avoid overwriting
                self.fileIndex += 1
                # Reset count of finished
                self.countFinished = 0
                # Reset done 
                self.writeQueue.clear()
        finally:
            self.writeSemaphore.release()