import collections
import os, sys, argparse, time

# from base_prefetcher import BasePrefetcher
# from span_prefetcher import SpanPrefetcher
# from cycle_prefetcher import SpanPrefetcher

class cache:
	def __init__(self, size, water_level = 0.9):
		self.hitsPage = 0
		self.missPage = 0
		self.ioHits = 0
		self.ioMiss = 0
		self.cachePage = 0
		self.wastePage = 0
		self.prefetchPage = 0
		self.prefetchHitPage = 0
		self.lastHitPageRatio = 0.0
		self.wastePagePoped = 0
		self.lru = collections.OrderedDict()
		self.hotmap = collections.OrderedDict() # prefetch statistic
		self.cacheSize = size
		self.cacheThrd = water_level * self.cacheSize
		# self.unit_size = unit_size # page size

	''' page manipulation '''
	def isPageExist(self, key):
		""" Judge whether key is in cache """
		if (key in self.lru.keys()):
				return True
		return False
		
	# def isTimeStampExist(self, key):
	#     if (key in self.timeSequence.keys()):
	#         return True
	#     return False

	def isPageHit(self, key, ts):
		if (key in self.hotmap.keys()):
			value = self.hotmap[key]
			oldHotness = value['hotness']
			newCnt = value['cnt'] + 1
			oldTs = value['time']
			value['cnt'] = newCnt
			value['hotness'] = 1 + oldHotness * newCnt / (ts - oldTs)
			value['time'] = ts
			self.hotmap[key] = value
		else:
			value = {'cnt': 1, 'hotness': 1, 'time': ts}
			self.hotmap[key] = value
		if (self.isPageExist(key) == False):
				self.missPage = self.missPage + 1
				return False
		self.hitsPage = self.hitsPage + 1
		value = self.lru[key]
		value['hitCnt'] = value['hitCnt'] + 1
		return True

	def insertPage(self, key, ts, isPrefetch):
		if (self.isPageExist(key)):
			value = self.lru.pop(key)
			self.lru[key] = value
		else:
			if (key in self.hotmap.keys()):
				value = self.hotmap[key]
				oldHotness = value['hotness']
				newCnt = value['cnt'] + 1
				oldTs = value['time']
				value['cnt'] = newCnt
				value['hotness'] = 1 + oldHotness * newCnt / (ts - oldTs)
				value['time'] = ts
				self.hotmap[key] = value
			else:
				value = {'cnt': 1, 'hotness': 1, 'time': ts}
				self.hotmap[key] = value
			value = {'isPrefetch': isPrefetch, 'hitCnt': 0}
			self.lru[key] = value
			self.cachePage = self.cachePage + 1

	def evictPage(self):
		while (len(self.lru) > self.cacheThrd):  
			value = self.lru.popitem(last = False)[1]
			if (value['hitCnt'] == 0):
				self.wastePage = self.wastePage + 1
				if (value['isPrefetch'] == True):
					self.wastePagePoped = self.wastePagePoped + 1

	def prefetchLbaInfo(self):
		sortHotmap = collections.OrderedDict(sorted(self.hotmap.items(), key=lambda t:t[1]['hotness']))
		prefetchMaxSize = self.cacheThrd - len(self.lru)
		prefetchActualSize = 0
		keys = list(sortHotmap.keys())
		for i in range(len(keys)):
			key = keys[i]
			self.prefetchPage = self.prefetchPage + 1
			self.cachePage = self.cachePage + 1
			if (self.isPageExist(key)):
				value = self.lru.pop(key)
				value['isPrefetch'] = True
				self.lru[key] = value
				continue
			value = {'isPrefetch': True, 'hitCnt': 0}
			self.lru[key] = value
			prefetchActualSize += 1
			if (prefetchActualSize == prefetchMaxSize):
				break
					

	''' IO manipulation '''
	def isIOHit(self, lba, len, ts):
		isHit = True
		for i in range(int(len)):
			if (self.isPageHit(lba+i, ts) == False):
				isHit = False
		if isHit:
			self.ioHits += 1
		else:
			self.ioMiss += 1



	''' Public APIs '''
	# split lba info to pages, divide prefetched pages and workload pages
	def getPrefetchWasteRatio(self):
		wastePageInCache = 0
		for cachedItem in self.lru.items():
			value = cachedItem[1]
			if (value['isPrefetch'] == True and value['hitCnt'] == 0):
				wastePageInCache += 1
		self.wastePageSoFar = wastePageInCache + self.wastePagePoped
		if self.prefetchPage != 0:
			return self.wastePageSoFar / self.prefetchPage
		return 0

	def getWasteRatio(self):
		if self.cachePage != 0:
			return float(self.wastePage) / (self.cachePage)
		return 0

	def showResult(self):
		wasteRatio = self.getWasteRatio()
		prefetchWasteRatio = self.getPrefetchWasteRatio()
		pageHitRatio = float(self.hitsPage) / (self.hitsPage + self.missPage)
		if pageHitRatio - self.lastHitPageRatio <= 0.003:
			self.prefetchLbaInfo()
		self.lastHitPageRatio = pageHitRatio
		self.evictPage()	 
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
		print("wastePage:" + str(self.wastePage))
		print("wastePagePoped:" + str(self.wastePagePoped))
		print("\n")
		sys.stdout.flush()

def SimWorkload(workload, cacheInst):
	number_of_items_per_log_flush = 100000
	ts = 0
	for line in open(workload):
		t = line.replace("\n", "")
		tmp = t.split('\t')
		lba = int(tmp[0]) / 2
		len = int(tmp[1]) / 2
		io_type = int(tmp[2])
		ts += 1
		if io_type == 0:
			# Read
			cacheInst.isIOHit(lba, len, ts)
		else:
			# Write
			for i in range(int(len)):
				cacheInst.insertPage(lba + i, ts, False)

		# check whether it is necessaty to flush log at current time
		if ts // number_of_items_per_log_flush != (ts+1) // number_of_items_per_log_flush:
			print('\n -- Flush log after processing %d requests. \n' % (ts))
			cacheInst.showResult()

	cacheInst.showResult()


if __name__ == '__main__': 

	parser = argparse.ArgumentParser()
	# parser.add_argument('-p', '--prefetcher_name', type=str, default='base')
	# parser.add_argument('-l', '--log_path', type=str, default='./Logs/')
	parser.add_argument('-n', '--trace_file_id', type=int, default=1000)
	args = parser.parse_args()

	''' [1] create workload_list and cache_entity '''
	if args.trace_file_id == 1:
		workload = '/data/wangqing/github/KVell/cachesim/data/9319'
		# cacheInst = cache(size=7340032) # 30GB
		# cacheInst = cache(size=6291456) # 26GB
		# cacheInst = cache(size=5242880) # 22GB
		#cacheInst = cache(size=4194304) # 18GB
		# cacheInst = cache(size=3145728) # 14GB
		# cacheInst = cache(size=2097152) # 10GB
		cacheInst = cache(size=1048576) # 6GB
		# cacheInst = cache(size=524288) # 2GB

	sys.stdout.flush()

	''' [2] trigger testing '''
	start_time = time.time()
	SimWorkload(workload, cacheInst)
	end_time = time.time()
	print('Time Consumption: %.3f' % (end_time - start_time))
	del cacheInst
