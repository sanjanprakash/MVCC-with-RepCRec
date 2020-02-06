# Authors :
# Sanjan Prakash Kumar (spk363)

import sys, os
import traceback
from pprint import pprint
import xmlrpclib

class DES :

	'''
	Class that will serve as the driver of our simulation.
    It will parse the input file line-by-line and execute each request on our distributed system.
    Since each line in the input file occurs at a distinct time instance, we call our driver a
     Discrete Event Simulator (DES).
	'''

	def __init__(self, inputFileName) :

		'''
		args :
		- inputFileName 			-			The name of the input file containing requests that
												 comprise a simulation.

		Constructor to initialize all data members of DES class.
		
		Data members :
		- file 						-			The input file.
		- transactionManager 		-			The host site that also serves as the TM. We set up a 
												 connection with the host server created in the
												 TransactionManager class.

		'''

		self.file = open(inputFileName)
		self.transactionManager = xmlrpclib.ServerProxy('http://localhost:7777', allow_none = True)
		self.parse()

	def parse(self) :

		'''
		This function is called by the constructor of the DES class. It parses the input file
		 line-by-line and processes the respective requests through corresponding calls to
		 the TM.
		'''

		for request in self.file.readlines() :
			request = request.strip()

			if request :
				self.transactionManager.clockForward()

				command = request.strip().strip(')').split('(')
				method = command[0]
				arguments = command[1].split(',')

				try :
					operation = getattr(self, method)
					operation(*arguments)
				except Exception as e :
					print ("MissingMethod: Unknown method detected.\n",e)
					sys.exit(1)

	def begin(self, transactionID) :

		'''
		args :
		- transactionID 			- 		The unique ID of the transaction that is starting now.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "begin(T2)". Here, transactionID is "T2".
		'''

		print ("------------")
		print ("begin: " + str(transactionID))

		try :
			print (self.transactionManager.begin(transactionID.strip()))
		except Exception as e :
			print ("\n\nBeginException: ")
			print (traceback.format_exc())

	def beginRO(self, transactionID) :

		'''
		args :
		- transactionID 			- 		The unique ID of the transaction that is starting now.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "beginRO(T1)". Here, transactionID is "T1".
		'''

		print ("------------")
		print ("beginRO: " + str(transactionID))

		try :
			print (self.transactionManager.beginRO(transactionID.strip()))
		except Exception as e :
			print ("\n\nBeginROException: ")
			print (traceback.format_exc())

	def R(self, transactionID, varID) :

		'''
		args :
		- transactionID 			- 		The unique ID of the transaction that wishes to read.
		- varID 					- 		The unique ID of the variable this transaction wishes to read.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "R(T1,x1)". Here, transactionID is "T1" and varID is "x1".
		'''

		print ("\n------------\n")
		print ("R: " + str(transactionID) + " " + str(varID))
		print(self.transactionManager.read(transactionID.strip(), varID.strip()))

	def W(self, transactionID, varID, value) :

		'''
		args :
		- transactionID 			- 		The unique ID of the transaction that wishes to write.
		- varID 					- 		The unique ID of the variable this transaction wishes to write.
		- value 					-		The value to be written into the variable.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "W(T1,x1,30)". Here, transactionID is "T1", varID is "x1" and value is "30".
		'''
		
		print ("\n------------\n")
		print ("W: " + str(transactionID) + " " + str(varID) + " " + str(value))
		print (self.transactionManager.write(transactionID.strip(), varID.strip(), int(value)))

	def fail(self, siteID) :

		'''
		args :
		- siteID 					- 		The unique ID of failing client site.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "fail(3)". Here, siteID is "3".
		'''

		print ("------------")
		print ("fail: " + str(siteID))
		print (self.transactionManager.fail(int(siteID.strip())))

	def recover(self, siteID) :

		'''
		args :
		- siteID 					- 		The unique ID of recovering client site.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "recover(7)". Here, siteID is "7".
		'''
		
		print ("------------")
		print ("recover: " + str(siteID))
		print (self.transactionManager.recover(int(siteID.strip())))

	def end(self, transactionID) :

		'''
		args :
		- transactionID 			- 		The unique ID of the terminating transaction.

		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "end(T2)". Here, transactionID is "T2".
		'''

		print ("------------")
		print ("end: " + str(transactionID))
		print (self.transactionManager.end(transactionID.strip()))

	def dump(self, args) :

		'''
		This function is called inside DES.parse when we encounter a line in the input file that reads
		 for example, "dump()".
		'''

		print ("------------")
		print ("dump: ")
		pprint (self.transactionManager.dump())


if __name__ == '__main__':
	if len(sys.argv) != 2:
		print ("MissingInput: Please specify the name of the input file.")
		sys.exit(1)

	driver = DES(sys.argv[1])