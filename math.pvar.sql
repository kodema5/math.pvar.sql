\if :{?math_pvar_sql}
\else
\set math_pvar_sql true

create schema if not exists math;

create or replace function math.equ (
    a double precision,
    b double precision,
    e double precision default 1e-10
)
    returns boolean
    language sql
    strict
as $$
    select abs(a-b)<e
$$;

\ir src/math/pvar.sql
\ir src/math/pcorr.sql

\endif
