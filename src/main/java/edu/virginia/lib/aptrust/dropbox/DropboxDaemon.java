package edu.virginia.lib.aptrust.dropbox;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple long-running program that watches a set of dropbox directories and
 * copies eligible content to corresponding preservation directories.  Each directory
 * within the dropbox directory that starts with the pattern "\d+_" will be treated
 * as a watched dropbox directory.  Whenever a file and its corresponding md5 file is
 * placed into that directory the the file is copied to a directory (named after the 
 * prefix digits for the dropbox directory) and if the checksum matches, the original 
 * is deleted and the copy preserved.  Whenever information must be communicated to
 * the owner of the dropbox directory, log files will be made having names based on 
 * the original file.
 */
public class DropboxDaemon {
    
    final private static Logger LOGGER = LoggerFactory.getLogger(DropboxDaemon.class);
    
    public static void main(String [] args) throws IOException {
        if (args.length != 2) {
            System.err.println("DropboxDaemon requires two arguments: the dropbox directory and the preservation directory.");
            System.exit(-1);
        }
        File watchRoot = new File(args[0]);
        File presRoot = new File(args[1]);
        new DropboxDaemon(watchRoot, presRoot);
    }
    
    private File presRoot;
    private File watchRoot;
    
    private Map<File, DropboxWatcher> watcherMap;
    private Thread refreshThread;
    
    public DropboxDaemon(File watchRoot, File presRoot) throws IOException {
        this.watchRoot = watchRoot;
        this.presRoot = presRoot;
        watcherMap = new HashMap<File, DropboxWatcher>();
        
        
        refreshThread = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!Thread.interrupted()) {
                    try {
                        refreshConfigurations();
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                try {
                    shutdownWatchers();
                } catch (InterruptedException e) {
                    LOGGER.error("Interrupted while shutting down watchers!");
                }
            }});
        
        refreshThread.start();
    }
    
    public synchronized void refreshConfigurations() throws InterruptedException {
        // first remove watchers for missing files
        for (File dir : new ArrayList<File>(watcherMap.keySet())) {
             if (!dir.exists() || !dir.isDirectory()) {
                LOGGER.info("Removing watcher for missing directory " + dir.getAbsolutePath() + ".");
                watcherMap.get(dir).shutdown();
                watcherMap.remove(dir);
             }
        }
        
        // then, add watchers for known files
        for (File watchDir : watchRoot.listFiles()) {
            if (!watcherMap.containsKey(watchDir) && watchDir.isDirectory()) {
                final File presDir = getPresDir(watchDir); 
                if (presDir == null) {
                    LOGGER.debug("Unable to determine preservation directory for " + watchDir.getAbsolutePath() + ".");
                } else {
                    watcherMap.put(watchDir, new DropboxWatcher(watchDir, presDir, presRoot));
                }
            }
        }
    }
    
    public synchronized void shutdownWatchers() throws InterruptedException {
        refreshThread.interrupt();
        refreshThread.join();
        for (DropboxWatcher w : watcherMap.values()) {
            w.shutdown();
        }
    }
    
    /**
     * Gets (and creates if non-existent) the directory for preservation copies
     * of content from the watchDir.
     * @return the directory (which will exist) or null if no directory can be determined
     * or created. 
     */
    private File getPresDir(final File watchDir) {
        Matcher m = Pattern.compile("(\\d+)_.*").matcher(watchDir.getName());
        if (m.matches()) {
            final File result = new File(presRoot, m.group(1));
            result.mkdirs();
            return result;
        } else {
            return null;
        }
    }

}
