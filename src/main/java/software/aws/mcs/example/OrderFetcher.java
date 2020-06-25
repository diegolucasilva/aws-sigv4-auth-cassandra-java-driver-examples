package software.aws.mcs.examples;

/*-
 * #%L
 * AWS SigV4 Auth Java Driver 4.x Examples
 * %%
 * Copyright (C) 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * #L%
 */

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import software.aws.mcs.auth.SigV4AuthProvider;
import software.aws.mcs.example.Book;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrderFetcher {
    private final static String HOST_NAME_CASSANDRA="cassandra.sa-east-1.amazonws.com";
    private final static int CASSANDRA_PORT=9142;

    public static void main() throws IOException {
        cassandraTest();


    }

    private static void cassandraTest() throws IOException {
        // Both of these can be speficied in the configuration, but
        // are being done programmatically here for flexibility as
        // example code.
        SigV4AuthProvider provider = new SigV4AuthProvider(Regions.SA_EAST_1.getName());
        List<InetSocketAddress> contactPoints = Collections.singletonList(new InetSocketAddress(HOST_NAME_CASSANDRA, CASSANDRA_PORT));

        try (CqlSession session = CqlSession.builder().addContactPoints(contactPoints).withAuthProvider(provider).withLocalDatacenter(Regions.SA_EAST_1.getName()).build()) {

            insertCassandra(session);

            readCassandra(session);
        }
    }

    private static void insertCassandra(CqlSession session) throws IOException{

        List<Book> books = getObjectContentS3();
        System.out.println("Execution insert bookstore.books...");

        PreparedStatement insertBook = session.prepare("insert into bookstore.books"
                +"(isbn, title, author, pages, year_of_publication)"+ "values(?, ?, ?, ?, ?)");

        books.stream().forEach((Book book)->{
            BoundStatement boundStatement = insertBook.bind()
                    .setString(0,book.getIsbn())
                    .setString(1,book.getTitle())
                    .setString(2,book.getAuthor())
                    .setInt(3,book.getPages())
                    .setInt(4,book.getYear())
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            ResultSet resultSet = session.execute(boundStatement);
        });
        System.out.println("done");
    }

    private static void readCassandra(CqlSession session) {
        PreparedStatement prepared = session.prepare("select * from bookstore.books");

        System.out.println("Executing query bookstore.books...");
        // We use execute to send a query to Cassandra. This returns a ResultSet, which is essentially a collection
        // of Row objects.
        ResultSet rs = session.execute(prepared.bind());

        // Print the header
        for (Row row : rs) {
            System.out.println(" ISBP= "+row.getString("isbn")+
                    " AUTHOR= "+row.getString("author")+
                    " PAGES= "+row.getInt("pages")+
                    " TITLE= "+row.getString("title")+
                    " YEAR_OF_PUBLICATON= "+row.getInt("year_of_publication")
            );
        }
        System.out.println();
    }

    private static List<Book> getObjectContentS3() throws IOException {
        String bukectName = "my-bucket";
        String key = "cassandra-data-example.csv";
        S3Object fullObject = null;

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.SA_EAST_1)
                .build();
        System.out.println("Downloading an object");
        fullObject = s3Client.getObject(new GetObjectRequest(bukectName,key));
        System.out.println("Content-Type "+ fullObject.getObjectMetadata().getContentType());
        System.out.println("Content: ");
        List<Book> books = convertToBooks(fullObject.getObjectContent());
        return books;
    }

    private static List<Book> convertToBooks(S3ObjectInputStream objectContent) throws IOException {
        List<Book> books = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(objectContent));
        String line = null;

        while((line = reader.readLine()) != null){
            System.out.println(line);
            String[] item = line.split(";");
            Book book = new Book();
            book.setIsbn(item[0]);
            book.setTitle(item[1]);
            book.setAuthor(item[2]);
            book.setPages(Integer.parseInt(item[3]));
            book.setYear(Integer.parseInt(item[4]));
            books.add(book);
            line = null;
        }
        return books;
    }
}
