package edu.virginia.lib.aptrust.dropbox;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that represents a watcher on a single dropbox directory.  The watcher
 * watches for new files.  When a new file arrives and has a corresponding
 * .md5 checksum file, it is validated and moved to to preservation directory.
 * Various error conditions will result in exceptions being thrown or messages
 * being logged within the dropbox directory. 
 */
public class DropboxWatcher {
    
    final private static Logger LOGGER = LoggerFactory.getLogger(DropboxWatcher.class);

    final public static int POLL_INTERVAL = 60000;
    
    private Queue<File> fileQueue;
    
    private File dropboxDirectory;
    
    private File preservationDirectory;
    
    private File tempDirectory;
    
    private Thread watcherThread;
    
    private Thread moverThread;
    
    public DropboxWatcher(File watchDir, File destinationDir, File tempDirectory) {
        preservationDirectory = destinationDir;
        this.tempDirectory = tempDirectory;
        dropboxDirectory = watchDir;
        fileQueue = new LinkedList<File>();
        
        watcherThread = new Thread(new WatchRunnable());
        watcherThread.start();
        moverThread = new Thread(new MoveRunnable());
        moverThread.start();
    }
    
    public void shutdown() throws InterruptedException {
        watcherThread.interrupt();
        moverThread.interrupt();
        watcherThread.join();
        moverThread.join();
    }

    Pattern WINDOWS_MD5_FILE = Pattern.compile("MD5 hash of file.*([0-9a-f][0-9a-f]( [0-9a-f][0-9a-f]){15}+).*", Pattern.DOTALL);
    Pattern DEFAULT_MD5 = Pattern.compile(".*([0-9a-f][0-9a-f]{32}).*");
    
    /**
     * A quick check to determine if its worth the time to process a file.  To be 
     * worth the time (which involves copying and computing a checksum) a file must:
     * 1.  have a checksum file
     * 2.  not have been processed since the last change to either the file or the checksum
     */
    boolean isFileEligibleForProcessing(final File file) {
        final File md5File = new File(file.getParent(), file.getName() + ".md5");
        final File logFile = new File(file.getParent(), file.getName() + ".log");
        if (logFile.exists() && md5File.exists() && logFile.lastModified() > md5File.lastModified() && logFile.lastModified() > file.lastModified()) {
            // Skip this file because it was processed and a message was logged and nothing has changed since then...
            return false;
        } 
        return file.exists() && md5File.exists();
    }
    
    /**
     * Checks to determine if a file has completed using the following steps:
     * 
     * 1.  check to make sure there's a checksum file
     * 2.  copying the file to a temporary location in the preservation  storage and computing the checksum
     * 3.  if the checksum matches, move the original to its final spot and delete the original.
     * @throws IOException 
     * @throws NoSuchAlgorithmException 
     */
    boolean moveToPreservationStoreIfDone(File file) throws IOException {
        long modificationDate = file.lastModified();
        String providedChecksum = null;
        MessageDigest digest = null;
        final File md5File = new File(file.getParent(), file.getName() + ".md5");
        if (md5File.exists()) {
            providedChecksum = FileUtils.readFileToString(md5File).trim();
            Matcher m = WINDOWS_MD5_FILE.matcher(providedChecksum);
            if (m.matches()) {
                providedChecksum = m.group(1).replaceAll(" ", "");
            } else {
                m = DEFAULT_MD5.matcher(providedChecksum);
                if (m.matches()) {
                    providedChecksum = m.group(1);
                }
            }
            try {
                digest = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e.getMessage());
            }
        } else {
            // no support for other digest formats yet
            LOGGER.info("  No checksum file \"" + md5File + "\" found for " + file.getAbsolutePath() + ".");
            return false;
        }
        
        File destinationFile = new File(preservationDirectory, file.getName());
        if (destinationFile.exists()) {
            throw new RuntimeException("  A file with the name " + file.getName() + " already exists in the preservation store.");
        }
        File destinationTempFile = new File(tempDirectory, UUID.randomUUID().toString());
        if (destinationTempFile.exists()) {
            throw new RuntimeException("  A file with the name " + destinationTempFile.getName() + " already exists in the temp directory.");
        }

        FileOutputStream fos = new FileOutputStream(destinationTempFile);
        DigestOutputStream dos = new DigestOutputStream(fos, digest);
        try {
            FileInputStream fis = new FileInputStream(file);
            try {
                IOUtils.copy(fis,  dos);
                dos.close();
                LOGGER.info("  " + file.getAbsolutePath() + " copied to " + preservationDirectory.getPath() + ".");
                final String digestHex = toHexString(dos.getMessageDigest().digest());
                if (digestHex.equalsIgnoreCase(providedChecksum)) {
                    if (file.lastModified() != modificationDate) {
                        LOGGER.info("  " + file.getAbsolutePath() + " modified, transaction cancelled.");
                        destinationTempFile.delete();
                        return false;
                    } else {
                        if (!destinationTempFile.renameTo(destinationFile)) {
                            throw new RuntimeException("Unable to move " + destinationTempFile.getPath() + " to " + destinationFile.getPath() + "!");
                        }
                        file.delete();
                        LOGGER.info("  " + file.getAbsolutePath() + " deleted from " + dropboxDirectory.getPath() + ".");
                        return true;
                    }
                } else {
                    destinationTempFile.delete();
                    LOGGER.info("  " + file.getAbsolutePath() + " had a checksum mismatch: transaction cancelled.");
                    throw new RuntimeException("Checksum mismatch for " + file.getName() + "! (" + providedChecksum + " != " + digestHex + ")");
                }
            } finally {
                fis.close();
            }
        } finally {
            dos.close();
        }
    }
    
    public static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }


    private class WatchRunnable implements Runnable {
    
        
        /** 
         * A polling implementation.
         */
        @Override
        public void run() {
            while (!Thread.interrupted()) {
                LOGGER.debug("Checking for eligible files in " + dropboxDirectory.getPath());
                pollForNewEligibleFiles(dropboxDirectory);
                try {
                    Thread.sleep(POLL_INTERVAL);
                } catch (InterruptedException e) {
                    return;
                }
            }
            LOGGER.info("WatchRunnable stopped!");
        }
        
        public void pollForNewEligibleFiles(final File f) {
            if (f.isDirectory()) {
                for (final File child : f.listFiles()) {
                    pollForNewEligibleFiles(child);
                }
            } else {
                if (isFileEligibleForProcessing(f)) {
                    synchronized (fileQueue) {
                        fileQueue.add(f);
                    }
                }
            }
        }
        
        /*
        public void runWatching() {
            try {
                Path path = dropboxDirectory.toPath();
                WatchService watchService = path.getFileSystem().newWatchService();
                path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);
                
                LOGGER.info("Watching " + dropboxDirectory.getAbsolutePath());
    
                while (!Thread.interrupted()) {
                    WatchKey watchKey = watchService.take();
                    
                    for (final WatchEvent<?> event : watchKey.pollEvents()) {
                        if (!event.kind().name().equals("ENTRY_DELETE")) {
                            Path modificationPath = path.resolve((Path) event.context());
                            File modifiedFile = modificationPath.toFile();
                            if (modifiedFile.getName().endsWith(".md5")) {
                                File targetFile = new File(modifiedFile.getParent(), modifiedFile.getName().substring(0, modifiedFile.getName().length() - 4));
                                if (targetFile.exists()) {
                                    synchronized (fileQueue) {
                                        fileQueue.add(targetFile);
                                    }
                                }
                            } else {
                                File md5File = new File(modifiedFile.getAbsolutePath() + ".md5");
                                if (md5File.exists()) {
                                    synchronized (fileQueue) {
                                        fileQueue.add(modifiedFile);
                                    }
                                }
                            }
                            LOGGER.info("Modification to " + modificationPath.toFile().getAbsolutePath());
                        }
                    }
                    if (!watchKey.reset()) {
                        LOGGER.warn(dropboxDirectory.getAbsolutePath() + " no longer exists!");
                        break;
                    }
                }
     
            } catch (InterruptedException ex) {
                LOGGER.warn("Interrupted, stopping directory watching [" + dropboxDirectory.getAbsolutePath() + "].");
                return;
            } catch (IOException ex) {
                LOGGER.error("Unexpected exception.", ex);
                return;
            }
            LOGGER.info("Directory Watcher ended [" + dropboxDirectory.getAbsolutePath() + "]");
        }
        */
    }
    
    
    private class MoveRunnable implements Runnable {

        @Override
        public void run() {
            LOGGER.debug("File Move process started");
            while (!Thread.interrupted()) {
               try {
                   Set<File> waitLonger = new HashSet<File>();
                   Set<File> moveNow = new HashSet<File>();
                   long now = System.currentTimeMillis();
                   synchronized (fileQueue) {
                       for (File f : fileQueue) {
                           if (f.lastModified() < (now - 60000)) {
                               moveNow.add(f);
                               LOGGER.info("  " + f.getAbsolutePath() + " queued for move to preservation storage.");
                           } else {
                               waitLonger.add(f);
                               LOGGER.debug("  " + f.getAbsolutePath() + " was modified within the last minute, waiting in case more changes occur.");
                           }
                       }
                       fileQueue.clear();
                       fileQueue.addAll(waitLonger);
                   }
                   for (File f : moveNow) {
                       try {
                           if (moveToPreservationStoreIfDone(f)) {
                               synchronized(fileQueue) {
                                   fileQueue.remove(f);
                                   appendToErrorLog(f, "Successfully transferred to preservation storage.");
                               }
                           } else {
                               // fall through, it'll get moved next time
                           }
                       } catch (RuntimeException e) {
                           appendToErrorLog(f, e.getMessage() == null ? "Unexepcted error!" : e.getMessage());
                       } catch (IOException e) {
                           appendToErrorLog(f, e.getMessage() == null ? "Unexepcted error!" : e.getMessage());
                           LOGGER.error("IO Exception while copying " + f.getName() + "!", e);
                           synchronized(fileQueue) {
                               fileQueue.add(f);
                           }
                       }
                    }
                   Thread.sleep(5000);
           } catch (InterruptedException ex) {
               LOGGER.warn("Interrupted, stopping file move processor.");
               return;
           } catch (IOException ex) {
               LOGGER.error("IO Exception while writing log file!", ex);
           }
           }
            LOGGER.info("File Move process ended");
        }
        
    }
    
    private void appendToErrorLog(File file, String message) throws IOException {
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final String text = f.format(new Date()) + " " + message + "\n";
        FileUtils.writeStringToFile(new File(file.getParent(), file.getName() + ".log"), text, "UTF-8", true);
    }
    
}
