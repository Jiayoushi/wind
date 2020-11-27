package com.wind.nanodb.commands;


import com.wind.nanodb.server.NanoDBServer;
import com.wind.nanodb.storage.StorageManager;
import com.wind.nanodb.transactions.TransactionException;


/**
 * This class represents a command that commits a transaction, such as
 * <tt>COMMIT</tt> or <tt>COMMIT WORK</tt>.
 */
public class CommitTransactionCommand extends Command {
    public CommitTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(NanoDBServer server) throws ExecutionException {
        // Commit the transaction.
        try {
            StorageManager storageManager = server.getStorageManager();
            storageManager.getTransactionManager().commitTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
