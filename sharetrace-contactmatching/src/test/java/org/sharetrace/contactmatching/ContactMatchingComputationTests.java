package org.sharetrace.contactmatching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.location.TemporalLocation;

@ExtendWith(MockitoExtension.class)
public class ContactMatchingComputationTests {

  private static final int MATRIX_SIZE = 3;

  private static final int TOO_SMALL = 1;

  private static final Set<Entry<Integer, Integer>> EMPTY_SET = ImmutableSet.of();

  private static final Set<Entry<Integer, Integer>> UNIQUE_ENTRIES = ImmutableSet.of(
      new SimpleImmutableEntry<>(0, 1),
      new SimpleImmutableEntry<>(0, 2),
      new SimpleImmutableEntry<>(1, 2));

  private static final Instant TIME_1 = Instant.ofEpochSecond(100);

  private static final Instant TIME_3 = Instant.ofEpochSecond(300);

  private static final String ID_1 = "ID_1";

  private static final String ID_2 = "ID_2";

  private static final Duration OCCURRENCE_THRESHOLD = Duration.ofSeconds(200);

  private static final List<LocationHistory> NO_HISTORIES = ImmutableList.of();

  private static final Set<Contact> NO_CONTACT = ImmutableSet.of();

  private static final ImmutableSortedSet<Occurrence> OCCURRENCES = ImmutableSortedSet.of(
      Occurrence.builder().setDuration(OCCURRENCE_THRESHOLD).setTime(TIME_1).build());

  private static final String ENTRY_EXISTS = "Size of at least 2 results in non-empty set";

  private static final String TOO_SMALL_MSG = "Size must be at least 2 for non-empty set";

  private static final String EMPTY_HISTORY_MSG = "Contacts cannot exist with no ids";

  private static final String SAME_ID_MSG = "Contact cannot be formed with the same id";

  private static final String TOO_SHORT_MSG = "Occurrence must be at least " + OCCURRENCE_THRESHOLD;

  private static final String CONTACT_EXISTS_MSG =
      "Contact should exist since occurrence of at least " + OCCURRENCE_THRESHOLD + " exists";

  @Mock
  private TemporalLocation tl1;

  @Mock
  private TemporalLocation tl2;

  @Mock
  private TemporalLocation tl3;

  @Mock
  private LocationHistory lh123Id1;

  @Mock
  private LocationHistory lh13Id1;

  @Mock
  private LocationHistory lh13Id2;

  @Mock
  private LocationHistory lh121Id2;

  @Mock
  private Contact contact;

  private ImmutableSortedSet<TemporalLocation> tl121;

  private ImmutableSortedSet<TemporalLocation> tl123;

  private ImmutableSortedSet<TemporalLocation> tl13;

  private List<LocationHistory> noContactsDiffIds;

  private List<LocationHistory> noContactsSameId;

  private List<LocationHistory> withContact;

  private static ContactMatchingComputation computation;

  @BeforeAll
  static void setup() {
    computation = spy(new ContactMatchingComputation());
    when(computation.getDurationThreshold()).thenReturn(OCCURRENCE_THRESHOLD);
  }

  @Test
  final void getUniqueEntries_verifyGeneratedEntries_returnsOnlyUniqueEntries() {
    assertEquals(UNIQUE_ENTRIES, computation.getUniqueEntries(MATRIX_SIZE), ENTRY_EXISTS);
  }

  @Test
  final void getUniqueEntries_verifyTooSmallInput_returnsEmptySet() {
    assertEquals(EMPTY_SET, computation.getUniqueEntries(TOO_SMALL), TOO_SMALL_MSG);
  }

  @Test
  final void compute_withEmptyList_returnsNoContacts() {
    assertEquals(NO_CONTACT, computation.compute(NO_HISTORIES), EMPTY_HISTORY_MSG);
  }

  @Test
  final void compute_withSameId_returnsNoContacts() {
    tl123 = ImmutableSortedSet.of(tl1, tl2, tl3);
    when(lh123Id1.getId()).thenReturn(ID_1);
    when(lh123Id1.getHistory()).thenReturn(tl123);
    noContactsSameId = ImmutableList.of(lh123Id1, lh123Id1);
    assertEquals(NO_CONTACT, computation.compute(noContactsSameId), SAME_ID_MSG);
  }

  @Test
  final void compute_withDiffIds_returnsNoContacts() {
    tl123 = ImmutableSortedSet.of(tl1, tl2, tl3);
    tl121 = ImmutableSortedSet.of(tl1, tl2, tl1);
    when(lh123Id1.getId()).thenReturn(ID_1);
    when(lh123Id1.getHistory()).thenReturn(tl123);
    when(lh121Id2.getId()).thenReturn(ID_2);
    when(lh121Id2.getHistory()).thenReturn(tl121);
    noContactsDiffIds = ImmutableList.of(lh123Id1, lh121Id2);
    assertEquals(NO_CONTACT, computation.compute(noContactsDiffIds), TOO_SHORT_MSG);
  }

  @Test
  final void compute_withOccurrence_returnsContact() {
    when(tl1.getTime()).thenReturn(TIME_1);
    when(tl1.hasSameLocation(tl1)).thenReturn(true);
    when(tl3.getTime()).thenReturn(TIME_3);
    tl13 = ImmutableSortedSet.of(tl1, tl3);
    when(contact.getFirstUser()).thenReturn(ID_1);
    when(contact.getSecondUser()).thenReturn(ID_2);
    when(contact.getOccurrences()).thenReturn(OCCURRENCES);
    when(lh13Id1.getId()).thenReturn(ID_1);
    when(lh13Id1.getHistory()).thenReturn(tl13);
    when(lh13Id2.getId()).thenReturn(ID_2);
    when(lh13Id2.getHistory()).thenReturn(tl13);
    withContact = ImmutableList.of(lh13Id1, lh13Id2);
    Contact contactFound = ImmutableList.copyOf(computation.compute(withContact)).get(0);
    assertTrue(contact.getFirstUser().equals(contactFound.getFirstUser()) ||
        contact.getFirstUser().equals(contactFound.getSecondUser()), CONTACT_EXISTS_MSG);
    assertTrue(contact.getSecondUser().equals(contactFound.getFirstUser()) ||
        contact.getSecondUser().equals(contactFound.getSecondUser()), CONTACT_EXISTS_MSG);
    assertEquals(contact.getOccurrences(), contactFound.getOccurrences(), CONTACT_EXISTS_MSG);
  }
}
