package org.apache.phoenix.end2end;

import org.apache.phoenix.schema.types.LobStoreFactory;
import org.apache.phoenix.schema.types.S3BasedLobStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static org.apache.phoenix.query.QueryServices.LOB_STORE_IMPL;

/**
 * 0. Start ./bin/phoenix_sandbox.py and copy the port number
 * 1. Show both empty S3 buckets
 * 2. Run hackathonPart1
 * 3. Show all-animals bucket (should have all images uploaded to S3) --> Web UI S3
 * 4. Run hackathonPart2
 * 5. Show just-cats bucket (should have only cat images) --> Web UI S3
 */
public class Hackathon {

    private static final String CAT = "CAT";
    private static final String DOG = "DOG";
    private static final String CAT_IMAGES_DIR = "/images/cats";
    private static final String DOG_IMAGES_DIR = "/images/dogs";
    private static final String ALL_ANIMALS_BUCKET = "all-animals";
    private static final String ONLY_CATS_BUCKET = "just-cats";
    private static final String JDBC_URL_NO_PORT = "jdbc:phoenix:localhost:";
    private static final String ALL_IMAGES_TABLE = "ALL_IMAGES";
    private static final String JUST_CATS_TABLE = "JUST_CATS";
    private static final String CREATE_TABLE_DDL = "CREATE TABLE %s "
            + "(ID INTEGER NOT NULL PRIMARY KEY, IMAGE BLOB, SPECIES VARCHAR(10))";
    private static final String DROP_TABLE_DDL = "DROP TABLE IF EXISTS %s";
    private static final String UPSERT = "UPSERT INTO %s VALUES(?, ?, ?)";
    private static final String SELECT_SPECIFIC_IMAGES = "SELECT IMAGE FROM %s WHERE SPECIES='%s'";

    private static String url;
    private static Properties props;

    public void hackathonPart1() throws Exception {
        final List<InputStream> catStreams = getStreams(CAT_IMAGES_DIR);
        final List<InputStream> dogStreams = getStreams(DOG_IMAGES_DIR);

        S3BasedLobStore.setBucketName(ALL_ANIMALS_BUCKET);
        try (Connection conn = DriverManager.getConnection(url, props)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(CREATE_TABLE_DDL, ALL_IMAGES_TABLE));
            }
            final String upsertQuery = String.format(UPSERT, ALL_IMAGES_TABLE);
            // Upsert all cat images
            int endingId = upsertImagesFromStreams(conn, upsertQuery, catStreams, CAT, 0);
            // Upsert all dog images
            upsertImagesFromStreams(conn, upsertQuery, dogStreams, DOG, endingId);
        }
    }

    public void hackathonPart2() throws SQLException{
        // Query only cat images
        final String selectCatsQuery = String.format(SELECT_SPECIFIC_IMAGES, ALL_IMAGES_TABLE, CAT);
        final List<InputStream> queriedCatStreams = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(selectCatsQuery);
            while(rs.next()) {
                queriedCatStreams.add(rs.getBinaryStream(1));
            }
        }

        // Upsert only cat images into the other bucket
        S3BasedLobStore.setBucketName(ONLY_CATS_BUCKET);
        try (Connection conn = DriverManager.getConnection(url, props)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(CREATE_TABLE_DDL, JUST_CATS_TABLE));
            }
            final String upsertQuery = String.format(UPSERT, JUST_CATS_TABLE);
            // Upsert all cat images
            upsertImagesFromStreams(conn, upsertQuery, queriedCatStreams, CAT, 0);
        }
    }

    private static void cleanup() throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, props);
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(DROP_TABLE_DDL, ALL_IMAGES_TABLE));
            stmt.execute(String.format(DROP_TABLE_DDL, JUST_CATS_TABLE));
        }
    }

    public static Properties getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(LOB_STORE_IMPL, LobStoreFactory.SUPPORTED_FORMATS.S3.name());
        Properties p = new Properties();
        p.putAll(props);
        return p;
    }

    private int upsertImagesFromStreams(Connection conn, String upsertQuery,
            List<InputStream> streams, String species, int startingId) throws SQLException {
        int i = startingId;
        while(i < streams.size() + startingId) {
            try (PreparedStatement prepStmt = conn.prepareStatement(upsertQuery)) {
                prepStmt.setInt(1, i);
                prepStmt.setBlob(2, streams.get(i - startingId));
                prepStmt.setString(3, species);
                prepStmt.execute();
            }
            i++;
        }
        conn.commit();
        return i;
    }

    private List<InputStream> getStreams(String dirPath) throws URISyntaxException,
            FileNotFoundException {
        URL res = getClass().getResource(dirPath);
        assert res != null;
        File file = Paths.get(res.toURI()).toFile();
        List<InputStream> streams = new ArrayList<>();
        for (File f: Objects.requireNonNull(file.listFiles())) {
            streams.add(new FileInputStream(f));
        }
        return streams;
    }

    public static void main(String[] args) throws Exception {
        url =  JDBC_URL_NO_PORT + "56455";
        props = getProps();
        Hackathon hack = new Hackathon();
        try {
            hack.hackathonPart1();
            System.out.println("Part 1 Finished! Upserted BLOBs and uploaded them to S3!");
            hack.hackathonPart2();
            System.out.println("Part 2 Finished! Queried cat images and uploaded those to S3!");
        } finally {
            cleanup();
        }
    }

}
