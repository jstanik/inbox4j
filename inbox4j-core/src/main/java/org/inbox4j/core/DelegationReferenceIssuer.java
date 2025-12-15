/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.inbox4j.core;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

class DelegationReferenceIssuer {

  private static final SecretKeySpec KEY;
  private static final byte ALGORITHM_VERSION = 1;
  private static final SecureRandom RANDOM = new SecureRandom();

  static {
    try {
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      // The purpose of the cryptography here is not secrecy, but just to make the
      // DelegationReference opaque enough for the consumer. Therefor we do not need to keep the key
      // secret. We only need to make sure that for the ALGORITHM_VERSION=1 it stays the same.
      String constant = "delegation-reference-issuer"; // Don't change the value.
      byte[] hash = sha256.digest(constant.getBytes(StandardCharsets.UTF_8));
      byte[] keyBytes = new byte[16];
      System.arraycopy(hash, 0, keyBytes, 0, 16);
      KEY = new SecretKeySpec(keyBytes, "AES");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  DelegationReference issueReference(InboxMessage inboxMessage) {
    long id = inboxMessage.getId();
    int version = inboxMessage.getVersion();
    byte[] input = new byte[12];
    ByteBuffer.wrap(input).putLong(id).putInt(version);
    byte[] encrypted = encrypt(input);
    byte[] output = new byte[1 + encrypted.length];
    ByteBuffer.wrap(output).put(ALGORITHM_VERSION).put(encrypted);
    return DelegationReference.fromString(HexFormat.of().formatHex(output));
  }

  IdVersion dereference(DelegationReference reference) {
    String value = reference.toString();
    byte[] data = HexFormat.of().parseHex(value);
    if (data[0] != ALGORITHM_VERSION) {
      throw new IllegalArgumentException(
          "Invalid delegation reference. Unsupported version: " + data[0]);
    }
    byte[] encrypted = new byte[data.length - 1];
    System.arraycopy(data, 1, encrypted, 0, encrypted.length);
    byte[] plain = decrypt(encrypted);
    var buffer = ByteBuffer.wrap(plain);
    long id = buffer.getLong();
    int version = buffer.getInt();
    return new IdVersion(id, version);
  }

  private static byte[] encrypt(byte[] input) {
    try {
      GCMParameterSpec iv = generateIv();
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, KEY, iv);
      byte[] encrypted = cipher.doFinal(input);
      ByteBuffer buffer = ByteBuffer.allocate(iv.getIV().length + encrypted.length);
      buffer.put(iv.getIV()).put(encrypted);
      return buffer.array();
    } catch (NoSuchPaddingException
        | IllegalBlockSizeException
        | NoSuchAlgorithmException
        | InvalidAlgorithmParameterException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] decrypt(byte[] input) {
    try {
      byte[] iv = new byte[12];
      System.arraycopy(input, 0, iv, 0, iv.length);
      GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv);
      byte[] encrypted = new byte[input.length - iv.length];
      System.arraycopy(input, iv.length, encrypted, 0, encrypted.length);
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.DECRYPT_MODE, KEY, parameterSpec);
      return cipher.doFinal(encrypted);
    } catch (NoSuchPaddingException
        | IllegalBlockSizeException
        | NoSuchAlgorithmException
        | InvalidAlgorithmParameterException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new IllegalArgumentException("Invalid delegation reference", e);
    }
  }

  public static GCMParameterSpec generateIv() {
    byte[] iv = new byte[12];
    RANDOM.nextBytes(iv);
    return new GCMParameterSpec(128, iv);
  }

  record IdVersion(long id, int version) {}
}
