# Authors :
# Sanjan Prakash Kumar (spk363)

import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer
from collections import defaultdict

from Transaction import Transaction

class TransactionManager(object) :

    '''
    Class that will serve as a transaction manager (TM).
    It will translate read/write requests on variables to read/write requests on copies of those variables
     on an available site hold them, using the available copies algorithm.
    '''

    def __init__(self) :

        '''
        Constructor to initialize all data members of TransactionManager class.

        Data members :
        
        - _server               -       Server object representing the host server for the 10 client sites.
        - _clientSites          -       The 10 sites, as client servers.
        - _clock                -       The clock of the system.
        - _transactionSites     -       Dictionary to maintain list of sites accessed by each transaction.
        - _transactions         -       Dictionary to maintain each Transaction object as value where the key is the Transaction ID (T1, T2, ...).
        - _waitlist             -       List to maintain all waitlisted transactions.
        - _activeTransactions   -       Set of active transactions being managed and under conflict (all nodes in the conflict graph).
        - _conflictGraph        -       The conflict graph as an adjacency list.

        '''

        self._clientSites = {}
        self._connectAllClients()
        self._clock = 0
        self._transactionSites = {}
        self._transactions = {}
        self._waitlist = []
        self._activeTransactions = set()
        self._conflictGraph = defaultdict(list)
        self._createHost(7777)

    def _createHost(self, port = 7777) :

        '''
        args :
        - port                  -       The port at which to create a host server.

        This function is called by the constructor of the TransactionManager class.
        It creates a server that represents the host OR the TM. This server will be handling
         requests to all the 10 client sites. We also register functions with this server
         that will be used as each line in the input file (requests) is processed.
        '''

        self._server = SimpleXMLRPCServer(("localhost", port), allow_none = True)

        self._server.register_function(self.clockForward)
        self._server.register_function(self.begin)
        self._server.register_function(self.beginRO)
        self._server.register_function(self.read)
        self._server.register_function(self.write)
        self._server.register_function(self.fail)
        self._server.register_function(self.recover)
        self._server.register_function(self.end)
        self._server.register_function(self.dump)

        self._server.serve_forever()

    def clockForward(self) :

        '''
        This function is used to move the clock of the system by one unit.
        This is called for each line that is read from an input file.
        '''

        self._clock += 1

    def begin(self, txnID) :

        '''
        args :
        - txnID             -           The transaction ID of the transaction that wishes to begin

        This function is called when we encounter a line in the input file that reads for example, "begin(T2)".
         We create a new Transaction instance with ID equal to "T2" of Read-Write type (default).
        '''

        self._transactions[txnID] = Transaction(txnID, self._clock)
        self._transactionSites[txnID] = []
        return 'Began Tx %s with time_stamp %d' % (txnID, self._transactions[txnID].getTimeStamp())

    def beginRO(self, txnID) :

        '''
        args :
        - txnID             -           The transaction ID of the transaction that wishes to begin

        This function is called when we encounter a line in the input file that reads for example, "beginRO(T2)".
         We create a new Transaction instance with ID equal to "T2" of Read-Only type.
        '''

        self._transactions[txnID] = Transaction(txnID, self._clock, RW = False)
        return 'Began read-only Tx %s with time_stamp %d' % (txnID, self._transactions[txnID].getTimeStamp())

    def read(self, txnID, var) :

        '''
        args :
        - txnID                     -           The ID of the transaction being processed at present
        - var                       -           The variable to be read

        This function is called when a read request is encountered in the input file - something along the lines of
         "R(T1,x1)". In this example, 'txnID' would be "T1" and 'var' would be "x1".
        We begin by first fetching all the sites at which this variable resides by using TransactionManager._sitesHoldingVar.
         We then check and see if making a read would lead to a conflict. If it does, we head to 
         TransactionManager._detectDeadlock; otherwise, the read get executed successfully. However, if none of the
         relevant sites are up, then this read request is added to the waitlist.
        '''

        transaction = self._transactions[txnID]
        if not transaction.isAborted() :
            varID = int(var[1:])
            sites = self._sitesHoldingVar(varID)
            for s in sites :
                if self._clientSites[s].isUp() :
                    self._addTransactionSites(transaction, s)
                    readResult = self._clientSites[s].read(transaction, var)
                    
                    if readResult :
                        if readResult['status'] == 'success' :
                            if transaction.isWaiting() :
                                transaction.activate()
                            return 'Read var %s for Tx %s at time_stamp %d, value: %s' % (var, txnID, self._clock, repr(readResult['data']))
                        elif readResult['status'] == 'exception':
                            args = readResult['args']
                            return self._detectDeadlock(('read', txnID, var), args[0], args[1])
            # If we reach here, we weren't able to find a site to read from
            self._addWaitlist(('read', txnID, var))
            return 'Unable to read %s, no site available' % var
        else :
            return 'Tx %s is in aborted state' % txnID

    def write(self, txnID, var, value) :

        '''
        args :
        - txnID                     -           The ID of the transaction being processed at present
        - var                       -           The variable to be written into
        - value                     -           The value to be written

        This function is called when a write request is encountered in the input file - something along the lines of
         "W(T1,x1,30)". In this example, 'txnID' would be "T1", 'var' would be "x1" and 'value' would be "30".
        We begin by first fetching all the sites at which this variable resides by using TransactionManager._sitesHoldingVar.
         We then check to see if this transaction has a read-lock on the same variable. If it does, then we iterate over the
         waitlisted requests and check for the existence of a conflict, which would in turn take us to 
         TransactionManager._detectDeadlock. However, if no such conflict arises, we attempt to write at the first available
         client site. If a conflict arises now, we head to TransactionManager._detectDeadlock just like before. Otherwise, we
         proceed to make an uncommitted write on that site. 
        '''

        transaction = self._transactions[txnID]
        resultStr = ""

        if not transaction.isAborted() :
            varID = int(var[1:])
            siteIDs = self._sitesHoldingVar(varID)
            succeededWrites = 0

            for s in siteIDs :
                if self._clientSites[s].isUp() :
                    if self._clientSites[s].isReading(txnID, var) :
                        for command in self._waitlist :
                            if int(command[2][1:]) == varID and not transaction.isAborted() and txnID != command[1] :
                                if command[0] == 'write' :
                                    resultStr += self._detectDeadlock(('write', txnID, var, value), True, command[1])
                                else :
                                    resultStr += self._detectDeadlock(('write', txnID, var, value), False, command[1])
                    
                    if not transaction.isAborted() :
                        self._addTransactionSites(transaction, s)
                        writeResult = self._clientSites[s].write(transaction, var, int(value))

                        if writeResult['status'] == 'exception' :
                            args = writeResult['args']
                            return resultStr + "\n" + self._detectDeadlock(('write', txnID, var, value), args[0], args[1])
                        elif writeResult['status'] == 'success' :
                            succeededWrites += 1
            
            if succeededWrites > 0 :
                if transaction.isWaiting() :
                    transaction.activate()
                return resultStr + '\nWrote var %s for txn %s at time_stamp %d' % (var, txnID, self._clock)
            elif resultStr == "" :
                # Unable to find site to read from
                self._addWaitlist(('write', txnID, var, value))
                return resultStr + '\nUnable to write %s, no site available' % var
            else :
                return resultStr
        else :
            return resultStr + '\nTx %s is in aborted state' % txnID

    def fail(self, siteID) :

        '''
        args :
        - siteID                -           The ID of the site to fail

        This function is called when we encounter a line in the input file that reads, for example "fail(3)". 
         In this example, site number 3 is said to fail. As a result of this, we abort all those transactions
         that were active on site 3. We do so by invoking TransactionManager._abortSiteTransactions.
        '''

        if siteID in self._clientSites :
            self._clientSites[siteID].fail()
            resultStr = self._abortSiteTransactions(siteID)
            
            if resultStr :
                return resultStr + 'Site %s failed at time_stamp %d\n' % (siteID, self._clock)
            else :
                return 'Site %s failed at time_stamp %d\n' % (siteID, self._clock)
        else :
            return 'Unknown site %s' % siteID

    def recover(self, siteID) :

        '''
        args :
        - siteID                -           The ID of the site to be recovered

        This function is called when we encounter a line in the input file that reads, for example "recover(3)". 
         In this example, site number 3 is said to be recovering. We recover this site and check if any of the
         waitlisted requests can now be executed.
        '''

        self._clientSites[siteID].recover()
        resultStr = self._retryWaitingTransactions()
        
        if resultStr :
            return 'Site %s recovered at time_stamp %d\n' % (siteID, self._clock) + resultStr
        else :
            return 'Site %s recovered at time_stamp %d' % (siteID, self._clock)

    def end(self, txnID) :

        '''
        args :
        - txnID                -           The ID of the transaction to be terminated

        This function is called when we encounter a line in the input file that reads, for example "end(T3)". 
         If T3 is a Read-Write type transaction, we go to all the sites that T3 had access to. If any of these
         sites is down, we abort T3 right away. Otherwise, we commit the uncommitted values of all variables
         at all up-and-running sites before we 'abort' (in this context, it is a termination upon completion)
         the transaction and check if any of the waitlisted requests can be executed.
        '''

        transaction = self._transactions[txnID]

        if not transaction.isAborted() :
            if transaction.isReadWrite() :
                for s in self._transactionSites[txnID] :
                    if self._clientSites[s].isUp() :
                        self._clientSites[s].commit(transaction, self._clock)
                    else :
                        resultStr = self._abort(transaction)
                        if resultStr :
                            return 'One of the sites accessed by Tx failed; aborting\n' + resultStr
                        else :
                            return 'One of the sites accessed by Tx failed; aborting'
                
                transaction.abort()
                resultStr = self._retryWaitingTransactions()
                
                if resultStr :
                    return 'Ended Tx %s at time_stamp %d\n' % (txnID, self._clock) + resultStr
                else :
                    return 'Ended Tx %s at time_stamp %d' % (txnID, self._clock)
            else :
                return 'Ended Tx %s at time_stamp %d' % (txnID, self._clock)
        else :
            return 'Tx %s is in aborted state' % txnID

    def dump(self) :

        '''
        This function is called when we encounter a line in the input file that reads "dump()".
         This results in fetching the last committed values of all variables on all 10 client sites.
        '''

        result = {}

        for siteID, client in self._clientSites.iteritems() :
            result[str(siteID)] = client.dump()

        return result

    def _detectDeadlock(self, command, isWriteLocked, conflictingTransactions) :

        '''
        args :
        - command                       -       The current request being processed (Read or Write operation)
        - isWriteLocked                 -       True, if conflictingTransactions hold write-locks; False, otherwise
        - conflictingTransactions       -       The conflicting transactions with present transaction. A list of 
                                                    transaction IDs when isWriteLocked is false; a string, otherwise.

        This function is called inside TransactionManager.write and TransactionManager.write when a conflict
         is detected. A conflict for a transaction T would be detected if :
        - a variable that T wishes to read is write-locked by another transaction T'.
        - a variable that T wishes to write to is locked by another transaction T'.
        - T wishes to promote its read-lock on a variable to a write-lock, but cannot do so
            until all other transactions release their locks that they hold on that variable.
        - T wishes to promote its read-lock on a variable to a write-lock, but cannot do so
            until all other transactions in the waitlist that wish to access that variable terminate.

        The conflicting pair of transactions P = (T1, T2) is added as an edge to the conflict graph. A deadlock check is
         performed by checking for the existence of a cycle in the conflict graph. If a deadlock is detected, 
         the younger transaction (higher timestamp) in P is aborted. Else, the current request is added to the waitlist.
        '''

        txnID = command[1]
        transaction = self._transactions[txnID]
        
        if isinstance(conflictingTransactions, str) :
            conflictingTxn = self._transactions[conflictingTransactions]
            isDeadlocked = self._addConflictGraph(txnID, conflictingTransactions)            
            if isDeadlocked :
                if transaction.getTimeStamp() > conflictingTxn.getTimeStamp() :
                    self._removeConflictGraph(txnID)
                    return self._abort(transaction)
                else :
                    self._removeConflictGraph(conflictingTransactions)
                    return self._abort(conflictingTransactions)            
            else :
                self._addWaitlist(command)
                return 'Waitlisted Tx %s at time_stamp %d' % (txnID, self._clock)
        
        elif isinstance(conflictingTransactions, list) :
            for conflictingTxnID in conflictingTransactions:
                if conflictingTxnID != txnID :
                    conflictingTxn = self._transactions[conflictingTxnID]
                    isDeadlocked = self._addConflictGraph(txnID, conflictingTxnID)
                    if isDeadlocked :
                        if transaction.getTimeStamp() > conflictingTxn.getTimeStamp() :
                            self._removeConflictGraph(txnID)
                            return self._abort(transaction)
                        else : 
                            self._removeConflictGraph(conflictingTxnID)
                            return self._abort(conflictingTxn)
            
            self._addWaitlist(command)
            return 'Waitlisted transaction %s at time_stamp %d' % (txnID, self._clock)

    def _addConflictGraph(self, currentTransaction, conflictingTransaction) :

        '''
        args :
        - currentTransaction            -       The transaction ID of the current transaction being processed
        - conflictingTransaction        -       The transaction ID of the conflicting transaction

        This function adds the directed edge (currentTransaction, conflictingTransaction) to the conflict graph.
         This edge represents that 'currentTransaction' conflicts with 'conflictingTransaction' and needs to
         wait for it to terminate and release the locks that 'currentTransaction' needs to proceed. It also adds
         these two transactions to the set '_activeTransactions' just to help us keep track of all the nodes in
         the conflict graph. Upon inserting this edge, we check if a cycle now exists in the conflict graph,
         which is also an indicator of a deadlock.  
        '''

        self._activeTransactions.add(currentTransaction)
        self._activeTransactions.add(conflictingTransaction)
        self._conflictGraph[currentTransaction].append(conflictingTransaction)
        
        return self._isCyclic()

    def _removeConflictGraph(self, abortingTransaction) :

        '''
        args :
        - abortingTransaction          -        The transaction ID of the transaction to be aborted

        This function is called inside TransactionManager._detectDeadlock when a deadlock is found to exist and
         is resolved by choosing to abort the younger transaction. Before the transaction is aborted, we erase
         the node corresponding to this transaction from the conflict graph as well as all edges involving it. 
        '''
        
        # Removing node from conflict graph
        self._activeTransactions.remove(abortingTransaction)

        # Removing outgoing edges
        if abortingTransaction in self._conflictGraph :
            del self._conflictGraph[abortingTransaction]
        
        # Removing incoming edges
        for txn, conflicts in self._conflictGraph.items() :
            if abortingTransaction in conflicts :
                conflicts.remove(abortingTransaction)

    def _isCyclic(self) :

        '''
        Notable local variables :
        - visited           -       A boolean entry for each node in the conflict graph. True, if seen before; false, otherwise.
        - grey              -       A boolean entry for each node in the conflict graph. True, if still part of an active path; false, otherwise.

        This function uses a depth-first traversal through the conflict graph to detect a cycle.
        '''

        visited, grey = {}, {}

        for t in self._activeTransactions :
            visited[t] = False
            grey[t] = False

        for t in self._activeTransactions :
            if not visited[t] :
                if self._dfs(t, visited, grey) :
                    return True
            return False

    def _dfs(self, node, visited, grey) :

        '''
        args :
        - node                  -           The current node on the depth-first path
        - visited               -           Dictionary with an entry for each node, indicating if a node has been seen before or not
        - grey                  -           Dictionary with an entry for each node, indicating if a node is part of an active path or not

        This function performs a depth-first traversal through the conflict graph one node at a time. It marks each
         node that it comes across as 'visited' and looks to propagate a path. All nodes that are part of this path
         are colored 'grey'. If the traversal happens upon a node colored 'grey', it indicates the closure of the
         active path - a cycle - and returns true. However, if no such occurrences pop up, no cycle is present in 
         the graph and we return false. 
        '''
        
        visited[node] = True
        grey[node] = True
        
        for nbr in self._conflictGraph[node] :
            if not visited[nbr] :
                if self._dfs(nbr, visited, grey) :
                    return True
            elif grey[nbr] :
                return True
        
        grey[node] = False
        return False

    def _connectAllClients(self):

        '''
        This function is called by the constructor of the TransactionManager class.
        It sets up connections with each of the 10 client servers.
        We will be using these client servers as client sites in our simulation.
        '''

        for i in range(1, 11) :
            url = 'http://localhost:' + str(9089 + i)
            clientSite = xmlrpclib.ServerProxy(url, allow_none = True)
            self._clientSites[i] = clientSite

    def _sitesHoldingVar(self, varID) :

        '''
        args :
        - varID                     -       The ID of the variable. For example, the ID of x12 is 12.
        
        This function is called inside TransactionManager.read and Transaction.write. For both these operations,
         this function fetches all the target sites on which the variable whose ID is 'varID' resides. Variables
         with even 'varID's reside on all client sites; variables with odd 'varID's reside only on site number
         1 + (varID mod 10). 
        '''
        
        if (varID % 2) == 0 :
            return self._clientSites.keys()
        else :
            return [1 + (varID % 10)]

    def _addTransactionSites(self, transaction, siteID) :

        '''
        args :
        - transaction                   -           The transaction that wishes to access this site.
        - siteID                        -           The ID of the site to be accessed.

        This function is called in TransactionManager.read and TransactionManager.write where we are building an
         association between the transaction and the client sites that it needs to access by adding the necessary
         site IDs to the list of sites this transaction can access. We do this by modifying 
         TransactionManager._transactionSites.
        '''
        
        if transaction.isReadWrite() :
            txnID = transaction.getID()

            if not siteID in self._transactionSites[txnID] :
                self._transactionSites[txnID].append(siteID)

    def _retryWaitingTransactions(self) :

        '''
        This function is called each time some transaction is terminated. This is because the termination of some
         transaction could result in the releasing of some locks that were conflicting with some of the
         waitlisted requests, which implies that some requests in the waitlist can proceed without any conflicts.
         In other words, this function tries to execute each of the requests in the waitlist, if there are no 
         conflicts.
        '''

        resultStr = ""
        i = 0

        while i < len(self._waitlist) :
            operation = self._waitlist[i]
            txnID = operation[1]
            transaction = self._transactions[txnID]

            if operation[0] == 'write' :
                var, value = operation[2:]
                resultStr += '\n' + self.write(txnID, var, value)
            elif operation[0] == 'read' :
                var = operation[2]
                resultStr += '\n' + self.read(txnID, var)

            if not transaction.isWaiting() and operation in self._waitlist :
                self._waitlist.remove(operation)
            else :
                i += 1

        return resultStr

    def _addWaitlist(self, command) :

        '''
        args :
        - command                   -               The current request being processed (Read or Write operation)

        This function is called inside TransactionManager.read, TransactionManager.write and TransactionManager._detectDeadlock
         whenever the current transaction cannot immediately run due to the target client site being down or there being lock 
         conflicts with other transactions (without the presence of a deadlock). This will add the current transaction to 
         the waitlist, where it will wait until the lock that it could not acquire earlier is released by all conflicting
         transactions or the target client site recovers. 
        '''

        txnID = command[1]
        transaction = self._transactions[txnID]

        if not transaction.isWaiting() :
            self._waitlist.append(command)
            transaction.wait()

    def _abortSiteTransactions(self, siteID) :

        '''
        args :
        - siteID                    -           The ID of the failing site

        This function is called inside TransactionManager.fail, where upon the failure of a site we abort all
         transactions that were accessing variables on this site.
        '''

        resultStr = ""

        for txnID, sites in self._transactionSites.iteritems() :
            if siteID in sites :
                transaction = self._transactions[txnID]
                if not transaction.isAborted() :
                    resultStr += self._abort(transaction)

        return resultStr

    def _abort(self, transaction) :

        '''
        args :
        - transaction               -           An instance of Transaction to be aborted

        This function is called inside TransactionManager._detectDeadlock, TransactionManager._abortSiteTransactions
         and TransactionManager.end. All of these are instances of when a transaction is to be terminated - forcibly
         (an abort) or after its due course of completion. The transaction in question is aborted on all the client
         sites that it was active on and then is also aborted on its own in isolation. Upon doing this, we check if
         any of the waitlisted requests can be executed by invoking TransactionManager._retryWaitingTransactions.
        '''

        txnID = transaction.getID()

        if txnID in self._transactionSites :
            for s in self._transactionSites[txnID] :
                self._clientSites[s].abort(transaction)
                transaction.abort()
            resultStr = 'Aborted Tx %s at time_stamp %d' % (txnID, self._clock)
            resultStr += "\n" + self._retryWaitingTransactions()
            
            return resultStr
        else :
            return 'Tx %s not found on transaction manager' % txnID

if __name__ == '__main__' :
    TM = TransactionManager()