package com.ruchika.LambdaXMLParser;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;

public class LambdaXmlParserApplication implements RequestHandler<S3Event, String> {

    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.defaultClient();
    private final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
    private final String tableName = "Books";

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        try {
            // Iterate over the S3 event records
            for (S3Event.S3EventNotificationRecord record : s3Event.getRecords()) {
                String bucketName = record.getS3().getBucket().getName();
                String objectKey = record.getS3().getObject().getKey();

                // Get the XML file from S3
                S3Object s3Object = s3Client.getObject(bucketName, objectKey);
                InputStream inputStream = s3Object.getObjectContent();

                // Parse the XML document
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(inputStream);
                doc.getDocumentElement().normalize();

                XPath xPath = XPathFactory.newInstance().newXPath();
                String expression = "/GoodreadsResponse/book"; // XPath expression
                NodeList nodeList = (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);

                // Iterate over the NodeList and extract book information
                for (int i = 0; i < nodeList.getLength(); i++) {
                    Node node = nodeList.item(i);
                    if (node.getNodeType() == Node.ELEMENT_NODE) {
                        Element bookElement = (Element) node;

                        // Extract book information
                        String id = getElementValue(bookElement, "id");
                        String title = getElementValue(bookElement, "title");
                        String isbn = getElementValue(bookElement, "isbn");
                        String description = getElementValue(bookElement, "description");
                        String imageUrl = getElementValue(bookElement, "image_url");
                        int publicationYear = Integer.parseInt(getElementValue(bookElement, "publication_year"));
                        String publisher = getElementValue(bookElement, "publisher");
                        String languageCode = getElementValue(bookElement, "language_code");
                        String averageRating = getElementValue(bookElement, "average_rating");

                        NodeList authorNodeList = bookElement.getElementsByTagName("author");
                        String author = "";
                        if (authorNodeList.getLength() > 0) {
                            Element authorElement = (Element) authorNodeList.item(0);
                            author = getElementValue(authorElement, "name");
                        }

                        // Insert data into DynamoDB table
                        Table table = dynamoDB.getTable(tableName);
                        Item item = new Item()
                                .withPrimaryKey("id", id)
                                .withString("title", title)
                                .withString("author", author)
                                .withString("isbn", isbn)
                                .withString("description", description)
                                .withString("imageUrl", imageUrl)
                                .withNumber("publicationYear", publicationYear)
                                .withString("publisher", publisher)
                                .withString("languageCode", languageCode)
                                .withString("averageRating", averageRating);

                        table.putItem(item);
                    }
                }
            }

            return "XML parsing and DynamoDB insertion completed successfully";
        } catch (Exception e) {
            e.printStackTrace();
            return "Error occurred: " + e.getMessage();
        }
    }

    private String getElementValue(Element parentElement, String tagName) {
        NodeList nodeList = parentElement.getElementsByTagName(tagName);
        if (nodeList.getLength() > 0) {
            Node node = nodeList.item(0);
            if (node != null) {
                return node.getTextContent().trim();
            }
        }
        return "";
    }
}
