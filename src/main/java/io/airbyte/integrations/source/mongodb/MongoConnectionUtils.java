/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import static io.airbyte.integrations.source.mongodb.MongoConstants.DRIVER_NAME;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.airbyte.cdk.integrations.debezium.internals.mongodb.MongoDbDebeziumPropertiesManager;

import java.io.ByteArrayInputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.io.FileOutputStream;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Helper utility for building a {@link MongoClient}.
 */
public class MongoConnectionUtils {

  private static String CERTIFICATE_PATH = "/tmp/arash.jks";

  /**
   * Creates a new {@link MongoClient} from the source configuration.
   *
   * @param config The source's configuration.
   * @return The configured {@link MongoClient}.
   */
  public static MongoClient createMongoClient(final MongoDbSourceConfig config) {
    final ConnectionString mongoConnectionString = new ConnectionString(buildConnectionString(config));

    final MongoDriverInformation mongoDriverInformation = MongoDriverInformation.builder()
        .driverName(DRIVER_NAME)
        .build();

    final MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
        .applyConnectionString(mongoConnectionString)
        .readPreference(ReadPreference.secondaryPreferred());

    if (config.hasAuthCredentials()) {
      final String authSource = config.getAuthSource();
      final String user = URLEncoder.encode(config.getUsername(), StandardCharsets.UTF_8);
      final String password = config.getPassword();
      mongoClientSettingsBuilder.credential(MongoCredential.createCredential(user, authSource, password.toCharArray()));
    }

    try {
      // We can't use the PEM certificate directly, so we need to convert it to JKS
      // Airbyte is not support file upload, so we need to get the certificate from
      // the config as string and convert it to JKS file.
      convertToJKS(config.getCertificate(), config.getPrivateKey(), CERTIFICATE_PATH, config.getCertificatePass());

      System.setProperty("javax.net.ssl.trustStore", CERTIFICATE_PATH);
      System.setProperty("javax.net.ssl.trustStorePassword", config.getCertificatePass());
      System.setProperty("javax.net.ssl.keyStore", CERTIFICATE_PATH);
      System.setProperty("javax.net.ssl.keyStorePassword", config.getCertificatePass());
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

      return MongoClients.create(mongoClientSettingsBuilder.build(), mongoDriverInformation);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void convertToJKS(String pemCertificate, String pemPrivateKey, String keystorePath,
      String keystorePassword) throws Exception {
    // Load certificate
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    Certificate cert = certFactory.generateCertificate(new ByteArrayInputStream(pemCertificate.getBytes()));

    // Load private key
    byte[] encoded = Base64.getDecoder().decode(removePEMHeaders(pemPrivateKey));
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    Key privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(encoded));

    // Create keystore and add certificate with private key
    KeyStore keystore = KeyStore.getInstance("JKS");
    keystore.load(null, keystorePassword.toCharArray());
    keystore.setKeyEntry("alias", (PrivateKey) privateKey, keystorePassword.toCharArray(), new Certificate[] { cert });

    // Save keystore to the file
    try (FileOutputStream fos = new FileOutputStream(keystorePath)) {
      keystore.store(fos, keystorePassword.toCharArray());
    }
  }

  // Return the base64-encoded string without PEM headers and footers
  private static String removePEMHeaders(String pemData) {
    return pemData.replaceAll("-----BEGIN .*-----|-----END .*-----", "").replaceAll("\\s", "");
  }

  private static String buildConnectionString(final MongoDbSourceConfig config) {
    return MongoDbDebeziumPropertiesManager.buildConnectionString(config.rawConfig(), true);
  }

}
