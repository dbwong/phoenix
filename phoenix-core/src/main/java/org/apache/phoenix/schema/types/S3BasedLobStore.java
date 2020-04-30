package org.apache.phoenix.schema.types;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.IOException;
import java.io.InputStream;

public class S3BasedLobStore implements LobStore {

    private static final String ROOT_DIRECTORY = "/tmp/";
    private AmazonS3 s3client;
    private static String BUCKET_NAME = "all-animals";
    private static S3BasedLobStore INSTANCE = null;

    public synchronized static S3BasedLobStore getInstance() {
        if(INSTANCE == null){
            INSTANCE = new S3BasedLobStore();
        }
        return INSTANCE;
    }

    public static void setBucketName(String bucketName) {
        BUCKET_NAME = bucketName;
    }

    public S3BasedLobStore() {
        BasicAWSCredentials credentials = new BasicAWSCredentials("FakeAccessKey", "FakeSecretKey");
        s3client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(credentials))
                        .withRegion(Regions.US_WEST_2).build();
    }

    @Override
    public String putLob(InputStream lobStream) throws LobStoreException {
        String lobLocator = RandomStringUtils.randomAlphanumeric(15); //For now use lobLocator
        try {
            s3client.putObject(
                   new PutObjectRequest(BUCKET_NAME, lobLocator, lobStream, new ObjectMetadata()));
        } catch (Exception e) {
            throw new LobStoreException(e);
        }
        return lobLocator;
    }

    @Override
    public InputStream getLob(String lobLocator) throws LobStoreException {
        try (final S3Object s3Object = s3client.getObject(BUCKET_NAME, lobLocator);
             InputStream in = s3Object.getObjectContent()) {
            return IOUtils.toBufferedInputStream(in);
        } catch (final IOException e) {
            throw new LobStoreException(e);
        }
    }
}
