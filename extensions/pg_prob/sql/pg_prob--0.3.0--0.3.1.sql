-- pg_prob 0.3.0 -> 0.3.1
-- Functions that draw random numbers were IMMUTABLE, so a call with constant
-- arguments was constant-folded and every row got the same draw (#146).
-- They are VOLATILE now; a seeded call is still reproducible.

ALTER FUNCTION "correlated_pair"(Dist, Dist, double precision, INT, bigint) VOLATILE;
ALTER FUNCTION "correlated_sample"(Dist[], double precision[], INT, bigint) VOLATILE;
ALTER FUNCTION "correlation"(Dist, Dist, INT, bigint) VOLATILE;
ALTER FUNCTION "covariance"(Dist, Dist, INT, bigint) VOLATILE;
ALTER FUNCTION "mean"(Dist, INT, bigint) VOLATILE;
ALTER FUNCTION "percentile"(Dist, double precision, INT, bigint) VOLATILE;
ALTER FUNCTION "prob_above"(Dist, double precision, INT, bigint) VOLATILE;
ALTER FUNCTION "prob_below"(Dist, double precision, INT, bigint) VOLATILE;
ALTER FUNCTION "prob_between"(Dist, double precision, double precision, INT, bigint) VOLATILE;
ALTER FUNCTION "sample"(Dist, bigint) VOLATILE;
ALTER FUNCTION "samples"(Dist, INT, bigint) VOLATILE;
ALTER FUNCTION "stddev"(Dist, INT, bigint) VOLATILE;
ALTER FUNCTION "summarize"(Dist, INT, bigint) VOLATILE;
ALTER FUNCTION "variance"(Dist, INT, bigint) VOLATILE;
