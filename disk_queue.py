import logging
import os
import threading

from sqlitedict import SqliteDict

class DiskQueue():
    DIR_PATH = './diskqueue'
    IN_PROGRESS_DB_NAME = 'inprogress.sqlite'
    TODO_DB_NAME = 'todo.sqlite'
    SEEN_DB_NAME = 'seen.sqlite'

    def __init__(self, load: bool = False):
        self.iterLock = threading.Lock()

        if not os.path.exists(self.DIR_PATH):
            os.makedirs(self.DIR_PATH)

        if not load:
            for path in [self.IN_PROGRESS_DB_NAME, self.TODO_DB_NAME, self.SEEN_DB_NAME]:
                try:
                    os.remove('{}/{}'.format(self.DIR_PATH, path))
                except:
                    continue

        self.inProgress = SqliteDict('{}/{}'.format(self.DIR_PATH, self.IN_PROGRESS_DB_NAME), autocommit=True)
        self.todo = SqliteDict('{}/{}'.format(self.DIR_PATH, self.TODO_DB_NAME), autocommit=True)
        self.seen = SqliteDict('{}/{}'.format(self.DIR_PATH, self.SEEN_DB_NAME), autocommit=True)

        # If we need to load state, add everything that was in progress to the todo queue
        if load:
            for key in self.inProgress.iterkeys():
                self.todo[key] = True
                del self.inProgress[key]

    def Push(self, key):
        if (key not in self.todo) and (key not in self.inProgress) and (key not in self.seen):
            self.todo[key] = True

    def Next(self):
        toReturn = None
        with self.iterLock:
            toReturn = next(self.todo.keys(), None)
            if toReturn:
                self.inProgress[toReturn] = True
                del self.todo[toReturn]
        return toReturn

    def Done(self, key):
        self.seen[key] = True
        del self.inProgress[key]

    def Close(self):
        self.inProgress.close()
        self.todo.close()
        self.seen.close()

    def IsDone(self):
        tmp = False
        with self.iterLock:
            tmp = len(self.todo) == 0 and len(self.inProgress) == 0
        return tmp