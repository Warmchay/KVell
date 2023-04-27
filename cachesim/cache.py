import collections
import os, sys, argparse, time

# from base_prefetcher import BasePrefetcher
# from span_prefetcher import SpanPrefetcher
# from cycle_prefetcher import SpanPrefetcher

class cache:
    def __init__(self, size, unit_size, water_level = 0.9):
        self.hitsPage = 0
        self.missPage = 0
        self.ioHits = 0
        self.ioMiss = 0
        self.cachePage = 0
        self.wastePage = 0
        self.prefetchPage = 0
        self.prefetchHitPage = 0
        self.wastePagePoped = 0
        self.lru = collections.OrderedDict()
        self.cacheSize = size
        self.cacheThrd = water_level * self.cacheSize
        self.unit_size = unit_size # page size
        self.timeSequence = collections.OrderedDict()

    ''' page manipulation '''
    def isPageExist(self, key):
        """ Judge whether key is in cache """
        if (key in self.lru.keys()):
            return True
        return False
    
    def isTimeStampExist(self, key):
        if (key in self.timeSequence.keys()):
            return True
        return False

    def isPageHit(self, key):
        if (self.isPageExist(key) == False):
            self.missPage = self.missPage + 1
            return False
        self.hitsPage = self.hitsPage + 1
        value = self.lru[key]
        value['hitCnt'] = value['hitCnt'] + 1
        return True

    def insertPage(self, key, isPretch):
        if (self.isPageExist(key)):
            value = self.lru.pop(key)
            self.lru[key] = value
        else:
            value = {}
            value['isPretch'] = isPretch
            value['hitCnt'] = 0
            self.lru[key] = value
            if (isPretch == True):
                self.prefetchPage = self.prefetchPage + 1
            self.cachePage = self.cachePage + 1

    def evictPage(self):
        while (len(self.lru) > self.cacheThrd):  
            value = self.lru.popitem(last= False)[1]
            if (value['hitCnt'] == 0):
                self.wastePage = self.wastePage + 1
                if (value['isPretch'] == True):
                    self.wastePagePoped = self.wastePagePoped + 1


    ''' IO manipulation '''
    def ioSplitToPages(self, lbaInfo):
        chunks = []
        lba = lbaInfo['lba']
        len = lbaInfo['len']
        ctrId = lbaInfo['ctrId']
        begin = lba // self.unit_size
        end = (lba + len - 1) // self.unit_size
        for chunkId in range(begin, end + 1):
            chunks.append(ctrId + str(chunkId))
        return chunks

    def isIOExist(self, lbaInfo):
        ioHit = True
        pages = self.ioSplitToPages(lbaInfo)
        for i in range(len(pages)):
            if (self.isPageExist(pages[i]) == False):
                ioHit = False # if has one is not in cache, also return false
        return ioHit

    def isIoHit(self, lbaInfo):
        pages = self.ioSplitToPages(lbaInfo)
        ioHit = True
        for i in range(len(pages)):
            if (self.isPageHit(pages[i]) == False):
                ioHit = False # lba info = addr + len, which means sequential pages not exist, only once is false
        if (ioHit):
            self.ioHits = self.ioHits + 1
        else:
            self.ioMiss = self.ioMiss + 1
        return ioHit

    def getPrefetchedPages(self, pretchIOLbaInfos):
        prefetchedPages = []
        for i in range(len(pretchIOLbaInfos)):
            pretchIOLbaInfo = pretchIOLbaInfos[i]
            if self.isIOExist(pretchIOLbaInfo):
                continue
            pages = self.ioSplitToPages(pretchIOLbaInfo)
            for i in range(len(pages)):
                prefetchedPages.append(pages[i])
        return prefetchedPages


    ''' handle time info '''
    def insertTimeStamp(self, lbaInfo):
        timeStamp = lbaInfo['timeStamp']
        if self.isTimeStampExist(timeStamp):
            self.timeSequence[timeStamp].append(lbaInfo)
        else:
            self.timeSequence[timeStamp] = []
            
        
        

    ''' Public APIs '''
    # split lba info to pages, divide prefetched pages and workload pages
    def updateCacheStatus(self, requestedIO, pretchedIOs):
        prefetchedPages = self.getPrefetchedPages(pretchedIOs)
        requestedPages = self.ioSplitToPages(requestedIO)
        # self.updateCachePages(ioPages, candidatePages)
        for i in range(len(requestedPages)):
            key = requestedPages[i]
            self.insertPage(key, False)
            self.evictPage()
        for i in range(len(prefetchedPages)):
            key = prefetchedPages[i]
            self.insertPage(key, True)
            self.evictPage()

    def getPrefetchWasteRatio(self):
        wastePageInCache = 0
        for cachedItem in self.lru.items():
            value = cachedItem[1]
            if (value['isPretch'] == True and value['hitCnt'] == 0):
                wastePageInCache += 1
        wastePageSoFar = wastePageInCache + self.wastePagePoped
        if self.prefetchPage != 0:
            return float(wastePageSoFar) / (self.prefetchPage)
        return 0

    def getWasteRatio(self):
        if self.cachePage != 0:
            return float(self.wastePage) / (self.cachePage)
        return 0

    def showResult(self):
        wasteRatio = self.getWasteRatio()
        prefetchWasteRatio = self.getPrefetchWasteRatio()
        pageHitRatio = float(self.hitsPage) / (self.hitsPage + self.missPage)
        ioHitRatio = float(self.ioHits) / (self.ioHits + self.ioMiss)
        print("lruCache: " + str(self.cacheSize))
        print("pageHitRatio: " + str(pageHitRatio))
        print("ioHitRatio: " + str(ioHitRatio))
        print("wasteRatio: " + str(wasteRatio))
        print("prefetchWasteRatio: " + str(prefetchWasteRatio))
        print("hitPage: " + str(self.hitsPage))
        print("missPage: " + str(self.missPage))
        print("ioHit: " + str(self.ioHits))
        print("ioMiss: " + str(self.ioMiss))
        print("\n")
        sys.stdout.flush()

# 将 workload 文本输入进来
def ReadWorkloadFromTxt(traceFile):
    f=open(traceFile, 'r')
    d=f.readlines()
    f.close()
    workload = []
    for i in range(len(d)):
        items = d[i].split(' ')
        time = int(items[0])
        addr = int(items[2]) # addr = int(d[i].split(' ')[2],16)
        size = int(items[3])
        ctrId = items[4]
        timeStamp = items[5] + items[6]
        lbaInfo = {'lunId':0, 'lba':addr, 'len':size, 'time':time, 'ctrId':ctrId, 'timeStamp':timeStamp}
        workload.append(lbaInfo)
    return workload

def SimWorkload(workload, cacheInst, prefetcher):

    number_of_items_per_log_flush = 10000

    for i in range(len(workload)):
        lbaInfo = workload[i]
        isIoHit = cacheInst.isIoHit(lbaInfo)
        cacheInst.insertTimeStamp(lbaInfo)
            
        prefetchLbaInfos = []
        if prefetcher != None:
            IO_request = {'time': lbaInfo['time'], 'addr': lbaInfo['lba'], 'size': lbaInfo['len'], 'ctrId': lbaInfo['ctrId'], 'timeStamp': lbaInfo['timeStamp']}
            interval_list_to_prefetch = prefetcher.prefetch_on_IO_request(IO_request, isIoHit)
            prefetchLbaInfos = [{'lunId': 0, 'lba': interval[0], 'len': interval[1]-interval[0], 'time': 1, 'ctrId': lbaInfo['ctrId'], 'timeStamp': lbaInfo['timeStamp']} for interval in interval_list_to_prefetch]
        cacheInst.updateCacheStatus(lbaInfo, prefetchLbaInfos)

        # check whether it is necessaty to flush log at current time
        if i // number_of_items_per_log_flush != (i+1) // number_of_items_per_log_flush:
            print('\n -- Flush log after processing %d requests. \n' % (i+1))
            cacheInst.showResult()

    cacheInst.showResult()


if __name__ == '__main__': 

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--prefetcher_name', type=str, default='base')
    parser.add_argument('-l', '--log_path', type=str, default='./Logs/')
    parser.add_argument('-n', '--trace_file_id', type=int, default=1000)
    args = parser.parse_args()

    ''' [1] create workload_list and cache_entity '''
    if args.trace_file_id == 1:
        traceFile = './Data/cpu_trace/spark.txt'
        workload = ReadWorkloadFromTxt(traceFile)
        cacheInst = cache(size=2097152, unit_size=65536) # unit page be 64KB

    elif args.trace_file_id == 2:
        traceFile = './Data/rocache_trace.txt'
        workload = ReadWorkloadFromTxt(traceFile)
        cacheInst = cache(size=2097152, unit_size=508) # unit be 64
    
    elif args.trace_file_id == 3:
        traceFile = './Data/kv_trace.txt'
        workload = ReadWorkloadFromTxt(traceFile)
        cacheInst = cache(size=249386, unit_size=508) # unit be 64
        
    elif args.trace_file_id == 4:
        traceFile = './Data/kv_trace.txt'
        workload = ReadWorkloadFromTxt(traceFile)
        cacheInst = cache(size=49878, unit_size=508) # unit be 64

    else: # sample trace
        print("example trace")
        traceFile = './Data/example_trace.txt'
        workload = ReadWorkloadFromTxt(traceFile)
        cacheInst = cache(size=2097152, unit_size=64) # unit be 64

    print(args)
    print('workload size: %d' % len(workload))
    sys.stdout.flush()

    ''' [2] specify prefetcher '''
    if args.prefetcher_name == 'span':
        # prefetcher = SpanPrefetcher(args.log_path)
        print('no span prefetcher')
    elif args.prefetcher_name == 'base': 
        # prefetcher = BasePrefetcher(args.log_path)
        print('no base prefetcher')
    else:
        prefetcher = None

    ''' [3] trigger testing '''
    start_time = time.time()
    SimWorkload(workload, cacheInst, prefetcher)
    end_time = time.time()
    print('Time Consumption: %.3f' % (end_time - start_time))
