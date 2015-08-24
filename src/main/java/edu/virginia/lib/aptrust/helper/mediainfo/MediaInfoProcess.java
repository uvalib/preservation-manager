package edu.virginia.lib.aptrust.helper.mediainfo;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * A java class that spawns a process running the MediaInfo command line program.
 */
public class MediaInfoProcess {

    private String mediaInfoCommandPath;
    
    public MediaInfoProcess() throws IOException {
        if (MediaInfoProcess.class.getClassLoader().getResource("conf/mediainfo.properties") != null) {
            Properties p = new Properties();
            p.load(MediaInfoProcess.class.getClassLoader().getResourceAsStream("conf/mediainfo.properties"));
            mediaInfoCommandPath = p.getProperty("mediainfo-command");
        } else {
            mediaInfoCommandPath = "mediainfo";
        }
    }
    
    public MediaInfoProcess(String path) {
        mediaInfoCommandPath = path;
    }
    
    public void generateMediaInfoReport(File mediaFile, File outputFile) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(mediaInfoCommandPath, "-f", mediaFile.getAbsolutePath()).start();
        new Thread(new OutputDrainerThread(p.getInputStream(), new FileOutputStream(outputFile))).start();
        new Thread(new OutputDrainerThread(p.getErrorStream(), null)).start();
        int returnCode = p.waitFor();
        if (returnCode != 0) {
            throw new RuntimeException("Invalid return code for process!");
        }
    }
    
    public void generateMediaInfoReportEBUCore(File mediaFile, File outputFile) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(mediaInfoCommandPath, "--Output=EBUCore", mediaFile.getAbsolutePath()).start();
        new Thread(new OutputDrainerThread(p.getInputStream(), new FileOutputStream(outputFile))).start();
        new Thread(new OutputDrainerThread(p.getErrorStream(), null)).start();
        int returnCode = p.waitFor();
        if (returnCode != 0) {
            throw new RuntimeException("Invalid return code for process!");
        }
    }
    
}
