package edu.virginia.lib.aptrust;

/**
 * Created by md5wz on 9/10/15.
 */
public interface RdfConstants {

    String UVA_PRESERVATION_NAMESPACE = "http://fedora.lib.virginia.edu/preservation#";
    String SKOS_NAMESPACE = "http://www.w3.org/2004/02/skos/core#";
    String PCDM_NAMESPACE = "http://pcdm.org/models#";
    String PREMIS_NAMESPACE = "http://www.loc.gov/premis/rdf/v1#";
    
    String FEDORA_BINARY = "http://fedora.info/definitions/v4/repository#Binary";
    
    /**
     * The class URI for external resources.
     */
    String EXTERNAL_RESOURCE_TYPE = UVA_PRESERVATION_NAMESPACE + "ExternalResource";

    /**
     * The unique (within the external system) identifier for an item managed by
     * an external system.  This is meant to be a literal property.
     */
    String EXTERNAL_ID = UVA_PRESERVATION_NAMESPACE + "externalId";

    /**
     * An identifier for an external system (that mints unique identifiers).  This
     * is meant to be a resource property (URI).
     */
    String EXTERNAL_SYSTEM = UVA_PRESERVATION_NAMESPACE + "externalSystem";

    /**
     * The class (rdf:type) of a resource representing an instance of an external
     * System.
     */
    String EXTERNAL_SYSTEM_TYPE = UVA_PRESERVATION_NAMESPACE + "ExternalSystem";

    /**
     * Indicates a pres:File that corresponds to the external resource.  This is typically
     * a file on disk that is loosly coupled with the external system.
     */
    String HAS_FILE = UVA_PRESERVATION_NAMESPACE + "hasFile";

    /**
     * Is used to duplicate links in Fedora 3 resources.
     */
    String PCDM_HAS_MEMBER = PCDM_NAMESPACE + "hasMember";

    /**
     * A relationship to link derivative files to their master files.
     */
    String IS_DERIVED_FROM = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#isDerivedFrom";

    /**
     * The class URI for Files.
     */
    String FILE_TYPE = UVA_PRESERVATION_NAMESPACE + "File";

    /**
     * A predicate that links a pres:File resource to each contained technical metadata resource.
     */
    String HAS_TECH_MD = UVA_PRESERVATION_NAMESPACE + "hasTechnicalMetadata";

    /**
     * A predicate that links a pres:File resource to the single contained binary resource.
     */
    String HAS_BINARY = UVA_PRESERVATION_NAMESPACE + "hasBinary";
    
    /**
     * The class URI for Preservation Events.
     */
    String AP_TRUST_PRESERVATION_EVENT_TYPE = UVA_PRESERVATION_NAMESPACE + "APTrustEvent";
    String PREMIS_EVENT_TYPE = PREMIS_NAMESPACE + "Event";
    
    /**
     * premis:hasEvent
     */
    String PREMIS_HAS_EVENT = PREMIS_NAMESPACE + "hasEvent";
    
    /**
     * premis:hasEventOutcomeInformation
     */
    String PREMIS_HAS_EVENT_OUTCOME_INFORMATION = PREMIS_NAMESPACE + "hasEventOutcomeInformation";
    
    String PREMIS_HAS_EVENT_OUTCOME = PREMIS_NAMESPACE + "hasEventOutcome";
    
    String APTRUST_FAILED = "failed";
    
    /**
     * pres:APTrustEventOutcomeInformation (premis:EventOutcomeInformation)
     */
    String AP_TRUST_EVENT_OUTCOME_INFORMATION = UVA_PRESERVATION_NAMESPACE + "APTrustEventOutcomeInformation";
    
    /**
     * A predicate that links a resource to an event that was later discovered to have
     * failed.  This predicate should replace the premis:hasEvent predicate when the
     * discovery is made.
     * @deprecated
     */
    String PRES_HAS_FAILED_EVENT = UVA_PRESERVATION_NAMESPACE + "hasFailedEvent";
    
    String PREMIS_HAS_EVENT_TYPE = PREMIS_NAMESPACE + "hasEventType";
    
    String PREMIS_HAS_EVENT_DATE = PREMIS_NAMESPACE + "hasEventDateTime";

    String PRES_BAG_ID = UVA_PRESERVATION_NAMESPACE + "aptrustEtag";
    String PRES_BAG_SIZE = UVA_PRESERVATION_NAMESPACE + "bagSize";
    String PRES_BAG_PAYLOAD_SIZE = UVA_PRESERVATION_NAMESPACE + "bagPayloadSize";
    
    String PRES_HAS_BAG_MANIFEST = UVA_PRESERVATION_NAMESPACE + "hasBagManifest";
    
    /**
     * A predicate that links any resource with a pres:File resource whose binary describes
     * this resource.  This is typically used to point to Files that describe many resources in aggregate
     * like a spreadsheet or database dump.
     */
    String HAS_METADATA = UVA_PRESERVATION_NAMESPACE + "isDescribedWithin";

    String PRESERVATION_PACKAGE_TYPE = UVA_PRESERVATION_NAMESPACE + "PreservationPackage";


    String DCTERMS_DESCRIPTION = "http://purl.org/dc/terms/description";


    String DC_IDENTIFIER = "http://purl.org/dc/elements/1.1/identifier";
    String DC_TITLE = "http://purl.org/dc/elements/1.1/title";
    String FILENAME = "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#filename";



    String DEFINITION = SKOS_NAMESPACE + "definition";

    String FILE_URI = UVA_PRESERVATION_NAMESPACE + "fileLocation";

    String RIGHTS_STATEMENT = "http://purl.org/dc/terms/RightsStatement";
    String CONCEPT = "http://www.w3.org/2004/02/skos/core#Concept";
    String LDP_CONTAINS = "http://www.w3.org/ns/ldp#contains";
    String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    String HAS_VERSION = "http://purl.org/dc/terms/hasVersion";
    String RIGHTS = "http://purl.org/dc/terms/rights";

    /**
     * This is the filename prefix given to the Fedora non-RDF resource containing the binary for
     * a technical metadata file.
     */
    String TECH_MD_FILENAME = "technical-metadata";
}
