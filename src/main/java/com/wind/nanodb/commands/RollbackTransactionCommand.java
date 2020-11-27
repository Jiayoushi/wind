package com.wind.nanodb.commands;


import com.wind.nanodb.server.NanoDBServer;
import com.wind.nanodb.storage.StorageManager;
import com.wind.nanodb.transactions.TransactionException;


/**
 * This class represents a command that rolls back a transaction, such as
 * <tt>ROLLBACK</tt> or <tt>ROLLBACK WORK</tt>.
 */
public class RollbackTransactionCommand extends Command {
    public RollbackTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        // Roll back the transaction.
        try {
            StorageManager storageManager = server.getStorageManager();
            storageManager.getTransactionManager().rollbackTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
