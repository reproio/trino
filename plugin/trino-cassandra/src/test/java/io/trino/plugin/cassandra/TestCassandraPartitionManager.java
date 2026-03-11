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
package io.trino.plugin.cassandra;

import io.trino.spi.predicate.Range;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.cassandra.CassandraPartitionManager.expandDiscreteRange;
import static io.trino.plugin.cassandra.CassandraType.Kind;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCassandraPartitionManager
{
    @Test
    public void testExpandDiscreteRangeInclusive()
    {
        Range range = Range.range(BIGINT, 1L, true, 3L, true);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.BIGINT);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    public void testExpandDiscreteRangeExclusiveBounds()
    {
        Range range = Range.range(BIGINT, 0L, false, 4L, false);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.BIGINT);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    public void testExpandDiscreteRangeSingleValue()
    {
        Range range = Range.range(BIGINT, 5L, true, 5L, true);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.BIGINT);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactlyInAnyOrder(5L);
    }

    @Test
    public void testExpandDiscreteRangeIntType()
    {
        Range range = Range.range(INTEGER, 1L, true, 3L, true);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.INT);
        assertThat(result).isPresent();
        assertThat(result.get()).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    public void testExpandDiscreteRangeNonIntegerType()
    {
        Range range = Range.range(BIGINT, 1L, true, 3L, true);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.TEXT);
        assertThat(result).isEmpty();
    }

    @Test
    public void testExpandDiscreteRangeEmptyAfterBoundAdjustment()
    {
        Range range = Range.range(BIGINT, 1L, false, 2L, false);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.BIGINT);
        assertThat(result).isEmpty();
    }

    @Test
    public void testExpandDiscreteRangeUnbounded()
    {
        Range range = Range.greaterThanOrEqual(BIGINT, 1L);
        Optional<Set<Object>> result = expandDiscreteRange(range, Kind.BIGINT);
        assertThat(result).isEmpty();
    }
}
