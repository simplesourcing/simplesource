package io.simplesource.data;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NonEmptyListTest {

    @Test
    void getShouldReturnElementAtIndex2() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        assertThat(list.get(2)).isEqualTo(30);
    }

    @Test
    void getShouldThrowOutOfBoundExceptionWhenIndexIsLargerThanSizeOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> list.get(10));
    }

    @Test
    void mapShouldAdd100ToEachElementOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        NonEmptyList<Integer> actualResult = list.map(v -> 100 + v);

        assertThat(actualResult).containsExactly(110, 120, 130, 140);
    }

    @Test
    void headShouldReturnFirstElementOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        assertThat(list.head()).isEqualTo(10);
    }

    @Test
    void tailShouldReturnAllElementsOfListExceptHead() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        assertThat(list.tail()).containsExactly(20, 30, 40);
    }

    @Test
    void toListShouldReturnAllElementsOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        assertThat(list.toList()).containsExactly(10, 20, 30, 40);
    }

    @Test
    void lastShouldReturnLastElementOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(10, 20, 30, 40);

        assertThat(list.last()).isEqualTo(40);
    }

    @Test
    void foldShouldReturnResultOfDividingElementsOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(100, 10, 5);

        Integer actualResult = list.fold(v -> v, (v1, v2) -> v1 / v2);

        assertThat(actualResult).isEqualTo(2);
    }

    @Test
    void foldLeftShouldReturnResultOfDividingElementsOfList() {
        NonEmptyList<Integer> list = NonEmptyList.of(5, 10, 100);

        Integer actualResult = list.foldLeft(1, (v1, v2) -> v2 / v1);

        assertThat(actualResult).isEqualTo(50);
    }

    @Test
    void fromListShouldCreateNonEmptyListFromElementsInTheSameOrder() {
        Optional<NonEmptyList<Integer>> oList = NonEmptyList.fromList(Stream.of(100, 10, 5).collect(toList()));

        assertThat(oList.isPresent()).isTrue();
        oList.ifPresent(list -> assertThat(list).containsExactly(100, 10, 5));
    }

    @Test
    void fromListShouldNotCreateNELFromEmptyList() {
        Optional<NonEmptyList<Integer>> oList = NonEmptyList.fromList(new ArrayList<>());

        assertThat(oList.isPresent()).isFalse();
    }
}