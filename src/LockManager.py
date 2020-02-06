# Authors :
# Sudharshann D (sd3770)

class LockException(Exception) :

	'''
	Class used to raise an exception when a pair of conflicting transactions perform operations.	 
	'''
	
	def __init__(self, *args) :
		
		'''
		args :
		- *args 		-		When a transaction T is in conflict with another transaction T' that 
								 owns a write-lock on a variable that T wants to access, the first 
								 member of '*args' is True; if T' had a read-lock, it would be false.
								The second member of '*args' is either a string representing the
								 transaction that owns a write-lock on a variable that T wants to access,
								 or a list of strings representing a list of transactions that own a
								 read-lock on that variable.
		'''

		self.args = [a for a in args]

class LockManager :

	'''
	Class that will serve as a lock manager for all variables on a client site.
	It will be responsible for granting read-locks and write-locks to transactions if there are no
	 conflicts, or raising LockException if there is a conflict.
	'''
	
	def __init__(self, siteVariables) :

		'''
		args :
		- siteVariables 		-		The IDs for all the variables that reside on a client site.

		Constructor to initialize all data members of LockManager class.

		Data members :

		- siteVariables 		- 		List of IDs of all variables on a client site whose locks are to be managed.
		- readLockTable			-		Dictionary to represent the read-lock table for a site, where the keys are the IDs of variables
										 and the values are lists of transaction IDs of transactions that have read-locks on that 
										 particular variable.
		- writeLockTable 		-		Dictionary to represent the write-lock table for a site, where the keys are the IDs of variables
										 and the values are transaction IDs of transactions that have a write-lock on that particular
										 variable.

		'''
		
		self.siteVariables = siteVariables
		self.writeLockTable = {}
		self.readLockTable= {}
		self._initLockTables()

	def _initLockTables(self) :

		'''
		This function is called by the constructor of the LockManager class.
		It creates 'blank' lock tables.
		'''

		for varID in self.siteVariables :
			self.writeLockTable[varID] = None
			self.readLockTable[varID] = []

	def getReaders(self, varID) :

		'''
		args :
		- varID 				-			The unique ID associated with the variable in question.

		This function is called inside Site.isReading. It is used to retrieve the IDs of all transactions
		 that own a read-lock over this variable.
		'''

		if varID in self.readLockTable :
			return self.readLockTable[varID]

		return []

	def hasReadLock(self, transaction, varID) :

		'''
		args :
		- transaction 			- 			The transaction that we need to check, if it owns a read-lock on
											 this particular variable or not.
		- varID 				- 			The unique ID of the variable in question.

		Notable local variables :
		- readLockOwners		-			List of transaction IDs of transactions that own a read-lock on
											 this particular variable.

		Although this function has not been invoked anywhere in our simulation, for the sake of completeness (and
		 for debugging purposes) we let this remain here. This will chek if this transaction owns a read-lock
		 over this particular variable or not.
		'''

		readLockOwners = []

		if varID in self.readLockTable :
			readLockOwners = self.readLockTable[varID]

		return transaction['ID'] in readLockOwners

	def hasWriteLock(self, transaction, varID) :

		'''
		args :
		- transaction 			- 			The transaction that we need to check, if it owns a write-lock on
											 this particular variable or not.
		- varID 				- 			The unique ID of the variable in question.

		This function is called inside Site.read and Site.commit to check if this transaction owns a write-lock
		 on this variable or not.
		'''

		if varID in self.writeLockTable :
			return self.writeLockTable[varID] == transaction['ID']

		return False

	def acquireReadLock(self, transaction, varID) :

		'''
		args :
		- transaction 			-			The transaction that wishes to acquire a read-lock.
		- varID 				-			The unique ID of the variable that this transaction wants to acquire
											 a read-lock over.

		This function is called inside Site.read when a read-write transaction that does not own a
		 write-lock on the variable it wishes to read requests a read. If this transaction owns a 
		 write-lock on this variable or no other transaction owns a write-lock on this variable, we 
		 can assign the read-lock to this transaction. However, if some other transaction owns a 
		 write-lock on this variable, a conflict is said to arise and we raise a LockException.
		'''
		
		if not self.hasReadLock(transaction, varID) :
			if self.writeLockTable[varID] != None :
				if self.writeLockTable[varID] != transaction['ID'] :
					raise LockException(True, self.writeLockTable[varID]) # (there is a write lock, and who has it)
			
			self.readLockTable[varID].append(transaction['ID'])

	def acquireWriteLock(self, transaction, varID) :

		'''
		args :
		- transaction 			-			The transaction that wishes to acquire a write-lock.
		- varID 				-			The unique ID of the variable that this transaction wants to acquire
											 a write-lock over.

		This function is called inside Site.write when a read-write transaction requests a write.
		 A LockException is raised if some other transaction owns a read-lock or write-lock over
		 this variable. Otherwise, no conflict is said to arise and we can assign the write-lock
		 to this transaction.
		'''

		if not self.hasWriteLock(transaction, varID) :
			if self.writeLockTable[varID] != None :
				if self.writeLockTable[varID] != transaction['ID'] :
					raise LockException(True, self.writeLockTable[varID])
			elif self.readLockTable[varID] != [] :
				for txnID in self.readLockTable[varID] :
					if txnID != transaction['ID'] :
						raise LockException(False, self.readLockTable[varID])
			
			self.writeLockTable[varID] = transaction['ID']

	def releaseAllLocks(self, transaction = None) :

		'''
		args :
		- transaction 				- 			The transaction that is committing or is to be aborted.

		This function is called inside Site.fail, Site.commit and Site.abort.
		When a client site fails, we just reset the lock tables. Otherwise, if a transaction is committing
		 or is aborted, we release all the locks it owns.
		'''
		
		if not transaction :
			self._initLockTables()
		else :
			self._releaseWriteLocks(transaction)
			self._releaseReadLocks(transaction)

	def _releaseReadLocks(self, transaction) :

		'''
		args :
		- transaction 			-			The transaction that is releasing all its read-locks.

		This function is called inside LockManager.releaseAllLocks. It will delete the ID of this transaction
		 from the read-lock table entry for all variables on this client site.
		'''

		for varID in self.readLockTable.keys() :
			try :
				self.readLockTable[varID].remove(transaction['ID'])			
			except ValueError :
				continue

	def _releaseWriteLocks(self, transaction) :

		'''
		args :
		- transaction 			-			The transaction that is releasing all its write-locks.
		
		This function is called inside LockManager.releaseAllLocks. It will reset the ID of the transaction
		 holding a write-lock on a variable in the write-lock table to 'None' for all variables on this 
		 client site.
		'''

		for varID in self.writeLockTable.keys() :
			if self.writeLockTable[varID] == transaction['ID'] :
				self.writeLockTable[varID] = None