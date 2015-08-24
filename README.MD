UVA Preservation Manager for AP Trust ingest
============================================

This is code use for ingesting local content into a
fedora 4 instance that is meant to mange ingest into 
AP Trust.  That fedora 4 instance maintains proxy
records as well as metadata associated with AP Trust
ingest events.  Furthermore, master files are exposed
through federation.

The routines in this codebase have been used for
one-off ingests (into the Fedora 4 repo) and one-off
transfers to AP Trust, but ultimately will evolve 
into a messge driven workflow using Apache camel.

Usage
-----

Getting content into AP Trust is a two step process.

1.  Represent the resources in Fedora 4
2.  Send some subset or resources to AP Trust 

Whether a resource has been ingested or sent to AP 
Trust is easily queryable using the triplestore 
configured as an index of the Fedora 4 Repository.
As such, this code is written to wait for updates
to be propagated to that index before continuing
processing.  This has the twofold advantage of a)
always being able to query the state of the 
repository and b) identifying problems with index
update before they're compounded.

# Ingest

Update the ProductionIngest.java for your
particular need and run it.

# Transfer eligible bags to AP Trust

Update Bagger.java for your eligibility requirements
and run it.

`mvn clean compile exec:java -Dexec.mainClass=edu.virginia.lib.aptrust.bag.Bagger`

