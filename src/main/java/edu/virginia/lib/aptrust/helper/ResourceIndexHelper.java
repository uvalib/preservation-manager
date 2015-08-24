package edu.virginia.lib.aptrust.helper;

import com.yourmediashelf.fedora.client.FedoraClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by md5wz on 9/17/15.
 */
public class ResourceIndexHelper {

    /**
     * Gets the subjects of the given predicate for which the object is give given object.
     * For example, a relationship like "[subject] follows [object]" this method would always
     * return the subject that comes before the given object.
     * @param fc the fedora client that mediates access to fedora
     * @param objectPid the pid of the object that will have the given predicate relationship
     * to all subjects returned.
     * @param predicate the predicate to query
     * @return the URIs of the subjects that are related to the given object by the given
     * predicate
     */

    public static List<String> getSubjects(FedoraClient fc, String predicate, String objectPid) throws Exception {
        if (predicate == null) {
            throw new NullPointerException("predicate must not be null!");
        }
        String itqlQuery = "select $subject from <#ri> where $subject <" + predicate + "> <info:fedora/" + objectPid + ">";
        BufferedReader reader = new BufferedReader(new InputStreamReader(FedoraClient.riSearch(itqlQuery).lang("itql").format("simple").execute(fc).getEntityInputStream()));
        List<String> pids = new ArrayList<String>();
        String line = null;
        Pattern p = Pattern.compile("\\Qsubject : <info:fedora/\\E([^\\>]+)\\Q>\\E");
        while ((line = reader.readLine()) != null) {
            Matcher m = p.matcher(line);
            if (m.matches()) {
                pids.add(m.group(1));
            }
        }
        return pids;
    }
}
