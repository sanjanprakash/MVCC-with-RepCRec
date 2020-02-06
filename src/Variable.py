# Authors :
# Sudharshann D (sd3770)

class Variable(object) :

	'''
	Class that will serve as a variable (x1, x2, ..., x20).
	It will be used to manage the actions of a read, a write, a recovery and a
	 commit to a variable on a client site at the lowest level of abstraction.
	'''

	def __init__(self, varID, value) :

		'''
		args :
		- varID 					-			The unique ID to be associated with each transaction.
		- value 					-			The value to be written at first to a variable. For a variable with ID 'x{i}', the value is 10*i. 

		Constructor to initialize all data members of Variable class.

		Data members :
		
		- ID 						- 			The unique ID associated with each of the 20 variables. For example, the ID for x11 is 'x11'.
		- committedValues 			- 			List of tuples representing committed values to the variable, along with the time of commit and the committing transaction. 
		- lastUncommitted 			-			Tuple representing the last write made to the variable. It stores the value written and the ID of the transaction writing to it. 
		- isActive 					-			Status of the variable. True, by default. Set to False, immediately after a failed site recovers.

		'''
		
		self.ID = varID
		self.committedValues = [(0, ('default', value))]
		self.lastUncommitted = (None, None)
		self.isActive = True

	def readCommitted(self, transaction = None) :

		'''
		args :
		- transaction 			-		An instance of the Transaction class representing the transaction trying to read.
		
		This function is called inside Site.read when a read-write transaction that wishes to read a variable 
		 acquires a read lock on it. In this case, the transaction will get to read the last committed value.
		This function is also called inside Site.read when a read-only transaction wishes to read a variable.
		 In this case, the transaction will get to read the last committed value before this transaction
		 began.
		It is also called inside Site.dump where we just simply retrieve the last committed value of a variable.
		'''

		# For read-only transactions
		if transaction and not transaction['isRW'] :
			for time, value in self.committedValues :
				if time <= transaction['timeStamp'] :
					last = value
				else :
					break
			return last
		# 
		else :
			return self.committedValues[-1][1]

	def readUncommitted(self, transaction) :

		'''
		args :
		- transaction 			- 		An instance of the Transaction class representing the transaction trying to read.

		This function is called inside Site.read when the transaction that wishes to read a variable owns a
		 write lock on that variable. This also invariably means that this particular transaction must have
		 last written an uncommitted value to this variable and is thus allowed to read a value written by it.
		However, just to make our code fail-safe, we read the last committed value before this transaction
		 began if this transaction was not the one that had last made an uncommitted write.
		'''

		if self.lastUncommitted[0] == transaction['ID'] :
			return self.lastUncommitted[1]
		return self.readCommmitted(transaction)

	def write(self, transaction, value) :

		'''
		args :
		- transaction 			- 		An instance of the Transaction class representing the transaction trying to write.
		- value 				- 		Value to be written to the variable.

		This function is called inside Site.write when the transaction that wishes to write to a variable 
		 manages to acquire a write-lock on that variable. However, since this write is yet to be committed
		 (which would only occur if the transaction would naturally terminate), we store this value as the
		 most recent ready-to-be-committed value, i.e. Variable.lastUncommitted.
		'''

		self.lastUncommitted = (transaction['ID'], value)

	def recover(self) :

		'''
		This function is called inside Site.recover. This would only be applicable to variables that have 
		 multiple copies existing on multiple client sites. And once recovered, there is no value to be 
		 read from it just yet. This bit comes into play during a call to Site.read of a variable on a
		 recently recovered site. 
		'''

		self.isActive = False

	def isRecovering(self) :

		'''
		This function is called inside Site.read. Although a read is possible without any conflict, if it is
		 a read being performed on a recently recovered client site, then there is no value to read.
		'''

		return not self.isActive

	def isReplicated(self) :
		
		'''
		This function is called inside Site.recover when a previously down client site is back up again.
		 Since we can only truly recover variables that have copies existing on at least one other site,
		 we look for the even-indexed variables (x2, x4, ..., x20) as they exist on all sites to begin
		 with. Thus, this function returns True for all even-indexed variables; otherwise, False.
		'''

		index = int(self.ID[1:])
		return (index % 2) == 0

	def commit(self, timeStamp) :

		'''
		args :
		- timeStamp 		- 			The exact time instance of commit.

		This function is called inside Site.commit, which in turn is called when a transaction ends (not aborts).
		 We first mark the end of the life of the variable by making it inactive and then add the most recent 
		 ready-to-be-committed value (Variable.lastUncommitted) to the list of committed values for this
		 variable. 
		'''

		if not self.isActive :
			self.isActive = True

		self.committedValues.append([timeStamp, self.lastUncommitted])

	# def loadCommitted(self, values) :
	# 	'''
	# 	load the commited value
	# 	'''
	# 	self.committedValues = values