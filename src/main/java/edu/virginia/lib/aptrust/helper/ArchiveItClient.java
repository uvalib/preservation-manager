package edu.virginia.lib.aptrust.helper;

import static edu.virginia.lib.aptrust.helper.PropertiesHelper.getRequiredProperty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class ArchiveItClient {

    public static void main(String [] args) throws IOException, IllegalStateException, ParserConfigurationException, SAXException, XPathExpressionException {
        Properties p = new Properties();
        FileInputStream fis = new FileInputStream("archive-it-credentials.properties");
        try {
            p.load(fis);
        } finally {
            fis.close();
        }
        
        ArchiveItClient c = new ArchiveItClient(getRequiredProperty(p, "username"), getRequiredProperty(p, "password"));
        
        for (Crawl crawl : c.getCrawls("6341")) {
            System.out.println(crawl.getLabel());
            for (Warc w : crawl.getWarcs()) {
                System.out.println("  " + w.url + ", " + w.filename + ", " + w.md5);
            }
        }
    }
    
    final private static Logger LOGGER = LoggerFactory.getLogger(ArchiveItClient.class);
    
    private String username;
    
    private String password;
    
    private String baseUrl;
    
    public ArchiveItClient(final String username, final String password) {
        this.username = username;
        this.password = password;
        this.baseUrl = "https://partner.archive-it.org/cgi-bin/getarcs.pl";
    }

    public String getBaseUrl() {
        return this.baseUrl;
    }
    
    public List<Crawl> getCrawls(final String collectionId) throws IllegalStateException, IOException, ParserConfigurationException, SAXException, XPathExpressionException {
        final List<Crawl> results = new ArrayList<Crawl>();
        String url = baseUrl + "?c=" + collectionId;
        
        // fetch and parse the page
        Document doc = null;
        HttpGet get = new HttpGet(url);
        try {
            System.out.print("Requesting archive information from " + url + "...");
            final HttpResponse response = HttpHelper.createClient(username, password, new URL(baseUrl).getHost()).execute(get);
            if (response.getStatusLine().getStatusCode() == 401) {
                LOGGER.warn("Received a 401 Unauthorized response from " + url + ", you may configure authentication credentials in auth.properties!");
            }
            if (response.getStatusLine().getStatusCode() < 200 || response.getStatusLine().getStatusCode() >= 300) {
                throw new RuntimeException(response.getStatusLine() + " result from request to get " + url);
            }
            System.out.println(response.getStatusLine().getStatusCode());
            DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
            DocumentBuilder b = f.newDocumentBuilder();
            // the XML is not well-formed so we have to crop away some of the garbage to be able to parse it using XML libraries
            ByteArrayOutputStream content = new ByteArrayOutputStream();
            IOUtils.copy(response.getEntity().getContent(), content);
            String raw = content.toString("UTF-8");
            String cropped = raw.substring(raw.indexOf("<table>"), raw.lastIndexOf("</table>") + 8);
            System.out.println("<<<<<<<<>>>>>>>>");
            System.out.println(cropped);
            System.out.println("<<<<<<<<>>>>>>>>");
            /*
             * Here's what' we've been getting from the API:
             * <table>
             * <tr>
             * <th>Archive File</th>
             * <th>Size (bytes)</th>
             * <th>MD5</th>
             * </tr>
             * <tr><td colspan='3' style='background-color:#ddd;font-weight:bold;'><a name='2015-04'>April 2015</a></td></tr>
             * <tr>
             * <td><a href="/cgi-bin/getarcs.pl/ARCHIVEIT-5422-NONE-3329-20150421185626695-00000-wbgrp-crawl056.us.archive.org-6444.warc.gz">ARCHIVEIT-5422-NONE-3329-20150421185626695-00000-wbgrp-crawl056.us.archive.org-6444.warc.gz</a></td>
             * <td>445737702</td>
             * <td>55360004a3abbf810239f67fa54047cf</td>
             * </tr>
             * <tr><td colspan='3' style='background-color:#ddd;font-weight:bold;'><a name='2015-03'>March 2015</a></td></tr>
             * <tr>
             * <td><a href="/cgi-bin/getarcs.pl/ARCHIVEIT-5422-NONE-24556-20150310140840782-00000-wbgrp-crawl104.us.archive.org-6443.warc.gz">ARCHIVEIT-5422-NONE-24556-20150310140840782-00000-wbgrp-crawl104.us.archive.org-6443.warc.gz</a></td>
             * <td>134997993</td>
             * <td>c6af0b8f2fd38a9fbce25514c0e3e400</td>
             * </tr>
             * </table>
             */
            doc = b.parse(new ByteArrayInputStream(cropped.getBytes("UTF-8")));
            System.out.println("Parsed response document.");
        } finally {
            get.releaseConnection();
        }
        
        String crawlTitle = null;
        List<Warc> warcs = new ArrayList<Warc>();
        XPath xpath = XPathFactory.newInstance().newXPath();
        final NodeList nl = (NodeList) xpath.evaluate("table/tr", doc, XPathConstants.NODESET);
        for (int i = 0; i < nl.getLength(); i ++) {
            Element row = (Element) nl.item(i);
            NodeList tds = getTDsForRow(row);
            if (tds.getLength() == 1) {
                validateAndCompileCrawl(crawlTitle, warcs, results);
                crawlTitle = (String) xpath.evaluate("a/text()", tds.item(0), XPathConstants.STRING);
            } else if (tds.getLength() == 3) {
                warcs.add(parseWarcFromTDs(tds));
            }
        }
        validateAndCompileCrawl(crawlTitle, warcs, results);
        return results;
    }
    
    private void validateAndCompileCrawl(final String title, final List<Warc> warcs, List<Crawl> crawls) {
        if (title == null && warcs.isEmpty()) {
            return;
        } else if (title != null && warcs.isEmpty()) { 
            throw new RuntimeException("No warcs for \"" + title + "\"!");
        } else if (title == null && !warcs.isEmpty()) {
            throw new RuntimeException("No title for warcs!");
        } else {
            crawls.add(new Crawl(title, new ArrayList<Warc>(warcs)));
            warcs.clear();
        }
    }
    
    /**
     * Gets a list of the text values of the TD elements within the supplied "tr" element.  The 
     * lenght of the list indicates the number of cells.  For rows with just "th" (heading) elements
     * this method will return an empty list.
     * @throws XPathExpressionException 
     */
    private static NodeList getTDsForRow(Element row) throws XPathExpressionException {
        XPath xpath = XPathFactory.newInstance().newXPath();
        return (NodeList) xpath.evaluate("td", row, XPathConstants.NODESET);
        
    }
    
    public Warc parseWarcFromTDs(NodeList tds) throws MalformedURLException, XPathExpressionException {
        XPath xpath = XPathFactory.newInstance().newXPath();
        if (tds.getLength() != 3) {
            throw new RuntimeException();
        }
        final URL baseUrl = new URL(this.baseUrl);
        final String file = (String) xpath.evaluate("a/text()", tds.item(0), XPathConstants.STRING);
        final String url = new URL(baseUrl, (String) xpath.evaluate("a/@href", tds.item(0), XPathConstants.STRING)).toString();
        final String md5 = (String) xpath.evaluate("text()", tds.item(2), XPathConstants.STRING);
        return new Warc(file, url, md5);
    }

    public static class Crawl {
        
        private String label;
        
        private List<Warc> warcs;
        
        public Crawl(final String label, final List<Warc> warcs) {
            this.label = label;
            this.warcs = warcs;
        }
        
        public String getLabel() {
            return label;
        }
        
        public List<Warc> getWarcs() {
            return warcs;
        }
        
    }
    
    public static class Warc {
        
        private String filename;
        
        private String url;
        
        private String md5;
        
        public Warc(final String filename, final String url, final String md5) {
            this.filename = filename;
            this.url = url;
            this.md5 = md5;
        }
        
        public String getFilename() {
            return filename;
        }
        
        public String getURL() {
            return url;
        }
        
        public String getMD5() {
            return md5;
        }
    }
}
