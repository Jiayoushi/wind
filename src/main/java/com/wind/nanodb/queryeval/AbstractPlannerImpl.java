package com.wind.nanodb.queryeval;


import com.wind.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;


/**
 * This class contains implementation details that are common across all query
 * planners.  Planners are of course free to implement these operations
 * separately, but just about all planners have some common functionality, and
 * it's very helpful to implement that functionality once in an abstract base
 * class.
 */
public abstract class AbstractPlannerImpl implements Planner {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(AbstractPlannerImpl.class);


    /** The storage manager used during query planning. */
    protected StorageManager storageManager;


    /** Sets the storage manager to be used during query planning. */
    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }
}
