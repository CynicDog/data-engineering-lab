-- Reference
--  - https://docs.databricks.com/aws/en/optimizations/range-join

-- https://docs.databricks.com/aws/en/optimizations/range-join#point-in-interval-range-join
-- using BETWEEN expressions
SELECT *
FROM points JOIN ranges ON points.p BETWEEN ranges.start and ranges.end;
-- using inequality expressions
SELECT *
FROM points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end;
-- with fixed length interval
SELECT *
FROM points
JOIN ranges
  ON points.p >= ranges.start
 AND points.p <  ranges.start + 100;

-- https://docs.databricks.com/aws/en/optimizations/range-join#interval-overlap-range-join
-- overlap of [r1.start, r1.end] with [r2.start, r2.end]
SELECT *
FROM r1 JOIN r2 ON r1.start < r2.end AND r2.start < r1.end;

-- https://docs.databricks.com/aws/en/optimizations/range-join#range-join-optimization-1
-- The range join optimization is performed for joins that:
--  - Have a condition that can be interpreted as a point interval or interval overlap range join.
--  - All values involved in the range join conditions are of a numeric type (integral, floating point,
--  decimal), DATE or TIMESTAMP
--  - All values involved in the range join conditions are of the same type. In the case of the
--  decimal type, the values also need to bve of the same scale and precision.
--  - It is an INNER JOIN, or in case of point in interval range join, a LEFT OUTER JOIN
--  with point value on the left side, or RIGHT OUTER JOIN with point value on the right side.
--  - Have a bin size tuning parameter.
SELECT /*+ RANGE_JOIN(points, 10) */ *
FROM points JOIN ranges ON points.p >= ranges.start AND points.p < ranges.end;