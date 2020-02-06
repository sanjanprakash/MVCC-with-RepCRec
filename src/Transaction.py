# Authors :
# Sanjan Prakash Kumar (spk363)

class Transaction :

	'''
	Class that will serve as a transaction.
	It will be used to hold transaction identifiers such as the ID of the transaction, the type of
	 the transaction, the timestamp at the start of the transaction and the status of the
	 transaction (0 = Active; 1 = Aborted; 2 = Waiting).
	'''

	def __init__(self, txnID, timeStamp, RW = True, txnStatus = 0) :

		'''
		args :
		- txnID 					-			The unique ID to be associated with each transaction.
		- timeStamp 				- 			The time instance at which this transaction is to be created.
		- RW 						- 			Boolean value to indicate the type of transaction. True, if read-write; False, otherwise. 
		- txnStatus 				- 			Status of the transaction. 0, if active (default); 1, if aborted; 2, if waiting.

		Constructor to initialize all data members of Transaction class.

		Data members :
		
		- ID 						- 			The unique ID associated with each transaction. For example, the ID for T3 is 'T3'.
		- timeStamp 				- 			The time instance at which this transaction began. 
		- isRW 		 				-			Boolean value to indicate the type of transaction. True, if read-write; False, otherwise. 
		- status 					-			Status of the transaction. 0, if active; 1, if aborted; 2, if waiting.
		'''

		self.ID = txnID
		self.timeStamp = timeStamp
		self.isRW = RW
		self.status = txnStatus

	def getID(self) :

		'''
		This function is called inside TransactionManager._addTransactionSites and TransactionManager._abort to
		 retrieve the unique ID of the transaction.
		'''

		return self.ID

	def getTimeStamp(self) :

		'''
		This function is called inside TransactionManager.begin, TransactionManager.beginRO and
		 TransactionManager._detectDeadlock to retrieve the timestamp at which this transaction
		 began.
		'''

		return self.timeStamp

	def isReadWrite(self) :

		'''
		This function is called inside TransactionManager._addTransactionSites and TransactionManager.end
		 to check if this transaction is of type read-write or not (read-only).
		'''

		return self.isRW

	# def getStatus(self) :
	# 	'''
	# 	get the status of the transaction.
	# 	this will be either aborted, waiting or active
	# 	'''
	# 	return self.status

	def isActive(self) :

		'''
		Although this function has not been invoked anywhere, for the sake of completeness, we leave this in here.
		 It can be used to check if this transaction is active or not. 
		'''

		return self.status == 0

	def isAborted(self) :

		'''
		This function is called inside TransactionManager.read, TransactionManager.write, TransactionManager.end
		 and TransactionManager._abortSiteTransactions to check if this transaction is aborted or not.
		'''

		return self.status == 1

	def isWaiting(self) :

		'''
		This function is called inside TransactionManager.read, TransactionManager.write, 
		 TransactionManager._retryWaitingTransactions and TransactionManager._addWaitlist
		 to check if this transaction is in the waiting state (in the waitlist) or not.
		'''

		return self.status == 2

	def activate(self) :

		'''
		This function is called inside TransactionManager.read and TransactionManager.write when a transaction
		 in the waitlist is called into action. The status of this transaction is set to 0 (or active).
		'''

		if self.status != 0 :
			self.status = 0

	def abort(self) :

		'''
		This function is called inside TransactionManager.end and TransactionManager._abort when this transaction
		 reaches its natural end, when a site being accessed by this transaction fails or when a deadlock is
		 detected. The status of this transaction is set to 1 (or aborted).
		'''

		if self.status != 1 :
			self.status = 1

	def wait(self) :

		'''
		This function is called inside TransactionManager._addWaitlist just after this transaction has been
		 added to the waitlist. The status of this transaction is set to 2 (or waiting).
		'''

		if (self.status != 2) :
			self.status = 2


if __name__ == '__main__' :
	Transaction(0, 0, True, 0)