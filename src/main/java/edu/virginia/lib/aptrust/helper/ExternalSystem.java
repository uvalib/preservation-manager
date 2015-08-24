package edu.virginia.lib.aptrust.helper;

import java.net.URI;

/**
 * Created by md5wz on 9/11/15.
 */
public class ExternalSystem {

    private String id;

    private String description;

    private URI fedora4Uri;

    public ExternalSystem(String id, String description, URI fedora4Uri) {
        this.id = id;
        this.description = description;
        this.fedora4Uri = fedora4Uri;
    }

    public String getId() {
        return this.id;
    }

    public String getDescription() {
        return description;
    }

    public URI getFedora4Uri() {
        return fedora4Uri;
    }

}
