# Authors :
# Sudharshann D (sd3770)

import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer

from Variable import Variable
from LockManager import LockManager
from LockManager import LockException

class Site :

	'''
	Class that will serve as a client site (10 instances).
	It will be used to manage the operations of read, write and commit to the variables on a particular site. It
	 will also be used to simulate the failure and recovery of a site. Further, we can also retrieve the last
	 committed values of each of the variables when a site dump is issued.

	Variables held by each site :
	- Site 1 				- 		{x2, x4, x6, x8, x10, x12, x14, x16, x18, x20}
	- Site 2 				- 		{x1, x2, x4, x6, x8, x10, x11, x12, x14, x16, x18, x20}
	- Site 3 				- 		{x2, x4, x6, x8, x10, x12, x14, x16, x18, x20}
	- Site 4	 			- 		{x2, x3, x4, x6, x8, x10, x12, x13, x14, x16, x18, x20}
	- Site 5 				- 		{x2, x4, x6, x8, x10, x12, x14, x16, x18, x20}
	- Site 6 				- 		{x2, x4, x5, x6, x8, x10, x12, x14, x15, 16, x18, x20}
	- Site 7 				- 		{x2, x4, x6, x8, x10, x12, x14, x16, x18, x20}
	- Site 8 				- 		{x2, x4, x6, x7, x8, x10, x12, x14, x16, x17, x18, x20}
	- Site 9 				- 		{x2, x4, x6, x8, x10, x12, x14, x16, x18, x20}
	- Site 10 				- 		{x2, x4, x6, x8, x9, x10, x12, x14, x16, x18, x19, x20}
	'''

	def __init__(self, siteID, port) :

		'''
		args :
		- siteID			-		The unique identifier for a client site. An integer in the range [1,10].
		- port				-		The port at which to create a server for a client site.

		Constructor to initialize all data members of Site class.

		Data members :

		- clientServer  	-		Server object representing the client server for this particular site.
		- ID 				-		The unique ID associated with each of the 10 client sites. An integer in the range [1,10].		
		- isActive 			-		Status of the client site. True, by default. Set to False, immediately upon the failure of the site.	
		- siteVariables 	-		Dictionary to maintain each Variable object as value where the key is the Variable ID (x1, x2, ...).
		- lockManager 		-		LockManager object to manage the read/write locks over the variables on this particular site.
		
		'''

		self.ID = siteID
		self.isActive = True
		self._initVariables()
		self.lockManager = LockManager(self.siteVariables.keys())
		self._createClient(port)

	def _createClient(self, port) :

		'''
		args :
		- port 				-		The port at which to create a server for this client site.

		This function is called by the constructor of the Site class.
		It creates a server that represents a client site holding some variables. This server will be
		 handling requests to read, write and commit values to its variables. It will also simulate 
		 the failure and recovery of this site. We also register functions with this server that will
         be used for each of these requests.
		'''
		self.clientServer = SimpleXMLRPCServer(("localhost", port), allow_none = True)

		self.clientServer.register_function(self.getID)
		self.clientServer.register_function(self.isUp)
		self.clientServer.register_function(self.isReading)
		self.clientServer.register_function(self.read)
		self.clientServer.register_function(self.write)
		self.clientServer.register_function(self.fail)
		self.clientServer.register_function(self.recover)
		self.clientServer.register_function(self.commit)
		self.clientServer.register_function(self.dump)
		self.clientServer.register_function(self.abort)

		self.clientServer.serve_forever()

	def getID(self) :

		'''
		Although this function has not been invoked anywhere in our simulation, for the sake of completeness (and
		 for debugging purposes) we let this remain here. This will fetch the unique ID associated with this
		 particular site.
		'''

		return self.ID

	def isUp(self) :

		'''
		This function is called inside TransactionManager.read, TransactionManager.write and TransactionManager.end.
		 It is used to check if this site is still active/recovered or has failed.
		'''
		
		return self.isActive

	# To check if this transaction holds a read-lock on this variable
	def isReading(self, transactionID, varID) :
		
		'''
		args :
		- transactionID 			-			The ID of the transaction in question.
		- varID						-			The ID of the variable in question.

		This function is called inside TransactionManager.write. It is used to check if this transaction has a 
		 read-lock on this particular variable or not.
		'''
		
		readers = self.lockManager.getReaders(varID)
		return transactionID in readers

	def read(self, transaction, varID) :

		'''
		args :
		- transaction 		-		The transaction that wishes to perform a read on a variable on this site.
		- varID 			-		The unique ID of the variable whose value is to be read.

		This function is called inside TransactionManager.read and TransactionManager._retryWaitingTransactions.
		If this site has just recovered from a failure, there is no value to read. If this transaction owns a
		 write-lock on this variable, then it reads the value that it most recently wrote to it. Otherwise, it
		 reads the most-recently committed value before the start of this transaction. In this case, if this is
		 a read-only transaction, then there is no need to acquire a read-lock on this variable. However, if it
		 is a read-write transaction, then it must first acquire a read-lock before performing the read. While
		 acquiring the lock, it could conflict with some transaction that must own a write-lock on the same
		 variable. In this case, the read-lock cannot be acquired and a LockException is raised.
		A status report is returned to the TM, indicating the success/failure of the read request.
		'''

		try :
			if varID in self.siteVariables :
				if self.siteVariables[varID].isRecovering() :
					return {'status': 'success', 'data': None}
				if self.lockManager.hasWriteLock(transaction, varID) :
					return {'status': 'success', 'data': self.siteVariables[varID].readUncommitted(transaction)}
				else :
					if transaction['isRW'] :
						self.lockManager.acquireReadLock(transaction, varID)
					return {'status': 'success', 'data': self.siteVariables[varID].readCommitted(transaction)}
			else :
				return {'status': 'error', 'data': None}
		except LockException as LE :
			return {'status': 'exception', 'args': LE.args}

	def write(self, transaction, varID, value) :

		'''
		args :
		- transaction 		-		The transaction that wishes to perform a write on a variable on this site.
		- varID 			-		The unique ID of the variable to be written into.
		- value 			-		The value to be written into this variable.

		This function is called inside TransactionManager.write and TransactionManager._retryWaitingTransactions.
		Before performing the write on the variable, a write-lock over it must be acquired. If this transaction
		 conflicts with another transaction that owns a read-lock or write-lock over the same variable before
		 this, the write-lock cannot be acquired and a LockException is raised.
		A status report is returned to the TM, indicating the success/failure of the write request.
		'''

		try :
			if varID in self.siteVariables :
				self.lockManager.acquireWriteLock(transaction, varID)
				self.siteVariables[varID].write(transaction, value)
				return {'status': 'success'}
		except LockException as LE :
			return {'status': 'exception', 'args': LE.args}

	def fail(self) :

		'''
		This function is called inside TransactionManager.fail upon the failure of a client site. When that
		 happens, we release all the locks (both, read and write) owned by all transactions that were active
		 on this client site.
		'''

		if self.isUp() :
			self.isActive = False
			self.lockManager.releaseAllLocks()

	def recover(self) :

		'''
		This function is called inside TransactionManager.recover when a failed client site recovers. When that
		 happens, we recover all those variables that had originally existed on this site and that also have
		 separate copies existing on other client sites, i.e. even-indexed variables.
		'''

		if not self.isUp() :
			for varID, var in self.siteVariables.iteritems() :
				if var.isReplicated() :
					var.recover()

			self.isActive = True

	def commit(self, transaction, timestamp) :

		'''
		args :
		- transaction 				- 			The transaction that has reached its natural end and needs to commit its operations.
		- timestamp 				-			The time instance at which this transaction ends.

		This function is called inside TransactionManager.end. When a transaction reaches its natural end, we 
		 need to commit all the most-recent writes performed it. Further, since this transaction is ending, we
		 also release all the locks (both, read and write) owned by this transaction. 
		'''
		
		for varID in self.siteVariables.keys() :
			if (self.lockManager.hasWriteLock(transaction, varID)) :
				self.siteVariables[varID].commit(timestamp)

		self.lockManager.releaseAllLocks(transaction)

	def dump(self) :

		'''
		This function is called inside TransactionManager.dump. This will retrieve all the most recently
		 committed values of all the variables on this site. A dictionary is returned, where the keys are
		 the IDs of all the variables on this site and the values are their last committed values and the
		 IDs of the transactions to have made those commits.
		'''

		output = {}

		for varID, var in self.siteVariables.iteritems() :
			output[varID] = var.readCommitted()

		return output

	def abort(self, transaction) :

		'''
		args :
		- transaction 			-			The transaction being aborted on this site.

		This function is called inside TransactionManager._abort, which in turn is called when a transaction 
		 reaches its natural end or when it must be forcibly aborted either due to the failure of a site being
		 accessed by this transaction or due to the detection of a deadlock. In either case, all we do is
		 release all the locks over the variables of this site owned by this transaction.
		'''

		self.lockManager.releaseAllLocks(transaction)

	def _initVariables(self) :

		'''
        This function is called by the constructor of the Site class.
        It creates Variable objects for each of the variables that this site holds. It also initializes
         with some default values that depend on their IDs. We place the even-indexed variables in all 
         of the client sites, but we are more selective with the odd-indexed variables.
        '''
		
		self.siteVariables = {}

		for i in range(1, 21) :
			varID = 'x' + str(i)
			value = 10*i

			if (i%2 == 0) :		
				self.siteVariables[varID] = Variable(varID, value)
			elif (1 + (i%10) == self.ID) :
				self.siteVariables[varID] = Variable(varID, value)


if __name__ == '__main__' :
	if len(sys.argv) != 3 :
		print ("MissingInput: Please provide the site ID and port number at which to create a client server.")
	
	siteID = int(sys.argv[1])
	port = int(sys.argv[2])
	site = Site(siteID, port)			