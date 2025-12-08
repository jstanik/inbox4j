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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.inbox4j.core.DelegationReferenceIssuer.IdVersion;
import org.junit.jupiter.api.Test;

class DelegationReferenceIssuerTest {

  @Test
  void issueReference() {
    DelegationReferenceIssuer cut = new DelegationReferenceIssuer();

    InboxMessage inboxMessage = mock(InboxMessage.class);
    when(inboxMessage.getId()).thenReturn(256L);
    when(inboxMessage.getVersion()).thenReturn(3);

    DelegationReference actual = cut.issueReference(inboxMessage);
    String referenceValue = actual.toString();
    assertThat(referenceValue).matches("^01[0-9a-fA-F]{80}$");
  }

  @Test
  void dereference() {
    DelegationReferenceIssuer cut = new DelegationReferenceIssuer();

    long id = 443L;
    int version = 7;

    InboxMessage inboxMessage = mock(InboxMessage.class);
    when(inboxMessage.getId()).thenReturn(id);
    when(inboxMessage.getVersion()).thenReturn(version);

    DelegationReference reference = cut.issueReference(inboxMessage);
    IdVersion idVersion = cut.dereference(reference);

    assertThat(idVersion.id()).isEqualTo(id);
    assertThat(idVersion.version()).isEqualTo(version);
  }
}
