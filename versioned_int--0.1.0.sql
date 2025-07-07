CREATE TYPE versioned_int;

CREATE FUNCTION versioned_int_in(cstring)
    RETURNS versioned_int
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION make_versioned(versioned_int, BIGINT)
    RETURNS versioned_int
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;

CREATE FUNCTION versioned_int_out(versioned_int)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE versioned_int (
    internallength = VARIABLE,
    input = versioned_int_in,
    output = versioned_int_out,
    alignment = double,
    storage = extended
);

CREATE TYPE ts_int AS (
    ts TIMESTAMPTZ,
    value BIGINT
);

CREATE TYPE __int_history AS (updated_at TIMESTAMPTZ, value BIGINT);

CREATE FUNCTION get_history(versioned_int)
    RETURNS SETOF __int_history
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION versioned_int_at_time(versioned_int, TIMESTAMPTZ)
    RETURNS BIGINT
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION versioned_int_at_time_eq(versioned_int, ts_int)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION versioned_int_eq_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_neq_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_gt_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_ge_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;
    
CREATE FUNCTION versioned_int_lt_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;
    
CREATE FUNCTION versioned_int_le_bigint(versioned_int, BIGINT)
    RETURNS BOOLEAN
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bigint_eq_versioned_int(BIGINT, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bigint_neq_versioned_int(BIGINT, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bigint_gt_versioned_int(BIGINT, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bigint_ge_versioned_int(BIGINT, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bigint_lt_versioned_int(BIGINT, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION bigint_le_versioned_int(BIGINT, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;   

CREATE FUNCTION versioned_int_eq_versioned_int(versioned_int, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_neq_versioned_int(versioned_int, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_gt_versioned_int(versioned_int, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_ge_versioned_int(versioned_int, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_lt_versioned_int(versioned_int, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION versioned_int_le_versioned_int(versioned_int, versioned_int)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR @ (
    LEFTARG = versioned_int,
    RIGHTARG = TIMESTAMPTZ,
    PROCEDURE = versioned_int_at_time
);

CREATE OPERATOR @= (
    LEFTARG = versioned_int,
    RIGHTARG = ts_int,
    PROCEDURE = versioned_int_at_time_eq,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR = (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_eq_bigint,
    COMMUTATOR = '=',
    NEGATOR = '<>',
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);

CREATE OPERATOR <> (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_neq_bigint,
    COMMUTATOR = '<>',
    NEGATOR = '=',
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);

CREATE OPERATOR > (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_gt_bigint,
    COMMUTATOR = <,
    NEGATOR = <=,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);

CREATE OPERATOR >= (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_ge_bigint,
    COMMUTATOR = <=,
    NEGATOR = <,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);

CREATE OPERATOR < (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_lt_bigint,
    COMMUTATOR = >,
    NEGATOR = >=,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG = versioned_int,
    RIGHTARG = bigint,
    PROCEDURE = versioned_int_le_bigint,
    COMMUTATOR = >=,
    NEGATOR = >,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);

CREATE OPERATOR = (
    LEFTARG    = bigint,
    RIGHTARG   = versioned_int,
    PROCEDURE  = bigint_eq_versioned_int,
    COMMUTATOR = '=',
    NEGATOR    = '<>',
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel
);

CREATE OPERATOR <> (
    LEFTARG    = bigint,
    RIGHTARG   = versioned_int,
    PROCEDURE  = bigint_neq_versioned_int,
    COMMUTATOR = '<>',
    NEGATOR    = '=',
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = bigint,
    RIGHTARG   = versioned_int,
    PROCEDURE  = bigint_gt_versioned_int,
    COMMUTATOR = <,
    NEGATOR    = <=,
    RESTRICT   = scalargtsel,      -- var (bigint literal) > col
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = bigint,
    RIGHTARG   = versioned_int,
    PROCEDURE  = bigint_ge_versioned_int,
    COMMUTATOR = <=,
    NEGATOR    = <,
    RESTRICT   = scalargesel,
    JOIN       = scalargejoinsel
);

CREATE OPERATOR < (
    LEFTARG    = bigint,
    RIGHTARG   = versioned_int,
    PROCEDURE  = bigint_lt_versioned_int,
    COMMUTATOR = >,
    NEGATOR    = >=,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = bigint,
    RIGHTARG   = versioned_int,
    PROCEDURE  = bigint_le_versioned_int,
    COMMUTATOR = >=,
    NEGATOR    = >,
    RESTRICT   = scalarlesel,
    JOIN       = scalarlejoinsel
);

CREATE OPERATOR = (
    LEFTARG    = versioned_int,
    RIGHTARG   = versioned_int,
    PROCEDURE  = versioned_int_eq_versioned_int,
    COMMUTATOR = '=',
    NEGATOR    = '<>',
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel
);

CREATE OPERATOR <> (
    LEFTARG    = versioned_int,
    RIGHTARG   = versioned_int,
    PROCEDURE  = versioned_int_neq_versioned_int,
    COMMUTATOR = '<>',
    NEGATOR    = '=',
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = versioned_int,
    RIGHTARG   = versioned_int,
    PROCEDURE  = versioned_int_gt_versioned_int,
    COMMUTATOR = <,
    NEGATOR    = <=,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = versioned_int,
    RIGHTARG   = versioned_int,
    PROCEDURE  = versioned_int_ge_versioned_int,
    COMMUTATOR = <=,
    NEGATOR    = <,
    RESTRICT   = scalargesel,
    JOIN       = scalargejoinsel
);

CREATE OPERATOR < (
    LEFTARG    = versioned_int,
    RIGHTARG   = versioned_int,
    PROCEDURE  = versioned_int_lt_versioned_int,
    COMMUTATOR = >,
    NEGATOR    = >=,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = versioned_int,
    RIGHTARG   = versioned_int,
    PROCEDURE  = versioned_int_le_versioned_int,
    COMMUTATOR = >=,
    NEGATOR    = >,
    RESTRICT   = scalarlesel,
    JOIN       = scalarlejoinsel
);

CREATE OR REPLACE FUNCTION versioned_int_consistent(internal, versioned_int, smallint, oid, internal)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE TYPE verint_rect;

CREATE FUNCTION verint_rect_in(cstring)
    RETURNS verint_rect
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION verint_rect_out(verint_rect)
    RETURNS cstring
    AS 'MODULE_PATHNAME'
    LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE verint_rect (
    internallength = 32,
    input = verint_rect_in,
    output = verint_rect_out,
    alignment = double,
    storage = plain
);
CREATE OR REPLACE FUNCTION versioned_int_union(internal, internal)
RETURNS verint_rect
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION versioned_int_compress(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION versioned_int_penalty(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION versioned_int_same(verint_rect, verint_rect, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION versioned_int_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OPERATOR CLASS gist_versioned_int_ops
    DEFAULT FOR TYPE versioned_int USING gist AS
        OPERATOR        1           @= (versioned_int, ts_int),
        FUNCTION        1           versioned_int_consistent,
        FUNCTION        2           versioned_int_union,
        FUNCTION        3           versioned_int_compress,
        FUNCTION        5           versioned_int_penalty,
        FUNCTION        6           versioned_int_picksplit,
        FUNCTION        7           versioned_int_same,
        STORAGE verint_rect;
