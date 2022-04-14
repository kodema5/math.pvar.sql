------------------------------------------------------------------------------
-- pvar_t collects simple statistics for data
-- pvar_t + double precision for real-time
-- pvar_t + pvar_t for parallel

drop type if exists math.pvar_t cascade;
create type math.pvar_t as (
    n int,                   -- count of non-null items
    m1 double precision,     -- accumulate means
    m2 double precision,     -- square distance from mean
    min double precision,
    max double precision
);

create or replace function math.pvar_t ()
    returns math.pvar_t
    language sql
    immutable
as $$
    select (
        0,
        0.0, 0.0,
        null, null
    )::math.pvar_t
$$;

------------------------------------------------------------------------------
-- extracting values
--

create or replace function math.avg (
    a math.pvar_t
)
    returns double precision
    language sql
    strict
as $$
    select (a).m1
$$;

create or replace function math.count (
    a math.pvar_t
)
    returns double precision
    language sql
    strict
as $$
    select a.n
$$;

create or replace function math.min (
    a math.pvar_t
)
    returns double precision
    language sql
    strict
as $$
    select (a).min
$$;

create or replace function math.max (
    a math.pvar_t
)
    returns double precision
    language sql
    strict
as $$
    select (a).max
$$;

create or replace function math.range (
    a math.pvar_t
)
    returns double precision
    language sql
    strict
as $$
    select (a).max - (a).min
$$;

create or replace function math.var (
    a math.pvar_t,
    s boolean default true -- if sample
)
    returns double precision
    language sql
    strict
as $$
    select case
    when s and a.n = 1 then null
    when s then (a.m2 / (a.n - 1))
    else a.m2 / a.n
    end
$$;


create or replace function math.std (
    a math.pvar_t,
    s boolean default true -- if sample
)
    returns double precision
    language sql
    strict
as $$
    select sqrt(math.var(a, s))
$$;

create or replace function math.sum (
    a math.pvar_t
)
    returns double precision
    language sql
    strict
as $$
    select (a.m1 * a.n)
$$;

------------------------------------------------------------------------------
-- pvar_t + double precision
--
create or replace function math.pvar (
    a math.pvar_t,      -- accumulator
    x double precision -- input
)
    returns math.pvar_t
    language plpgsql
as $$
declare
    d1 double precision;
    d2 double precision;
begin
    -- process only non-null values
    if x is null then
        return a;
    end if;

    a = coalesce(a, math.pvar_t());

    a.n = a.n + 1;
    d1 = x - a.m1;
    a.m1 = a.m1 + d1 / a.n;
    d2 = x - a.m1;
    a.m2 = a.m2 + d1 * d2;
    a.min = least(a.min, x);
    a.max = greatest(a.max, x);

    return a;
end;
$$;

drop operator if exists + (math.pvar_t, double precision);
create operator + (
    leftarg = math.pvar_t,
    rightarg = double precision,
    procedure = math.pvar,
    commutator = +
);


\if :test
    create function tests.test_pvar_iterative() returns setof text language plpgsql as $$
    declare
        v double precision;
        a math.pvar_t;

        ok boolean = true;
    begin
        select var_samp(x) into v from (select unnest(array [1,2,.3]) x) t;
        a = a + 1 + 2 + .3;

        return next ok(
            math.equ(math.var(a), v),
            'pvar_t + double precision');
    end;
    $$;
\endif


------------------------------------------------------------------------------
-- agg (pvar_t,  double precision) -> pvar_t
--
create or replace aggregate math.pvar (
    double precision
)(
    sfunc = math.pvar, -- step-function
    stype = math.pvar_t
);


\if :test
    create function tests.test_pvar_agg() returns setof text language plpgsql as $$
    declare
        ok boolean;
    begin
        select
            math.equ(math.var(a), var_s)
            and math.equ(math.var(a, false), var_p)
            and math.equ(math.std(a), std_s)
            and math.equ(math.avg(a), m)
            and math.equ(math.sum(a), s)
        into ok
        from (
            select
                math.pvar(x) as a,
                var_pop(x) as var_p,
                var_samp(x) as var_s,
                stddev_pop(x) as std_p,
                stddev_samp(x) as std_s,
                avg(x) as m,
                sum(x) as s
            from (values (1), (null), (2), (10), (20), (50)) as t(x)
        ) as t;

        return next ok(ok, 'pvar_t ~ var, std, avg, sum');
    end;
    $$;
\endif

------------------------------------------------------------------------------
-- pvar_t + pvar_t = parv_t
-- sum(parv_t)

create or replace function math.pvar (
    a math.pvar_t,
    b math.pvar_t
)
    returns math.pvar_t
    language plpgsql
as $$
declare
    n int;
    d double precision;
begin
    a = coalesce(a, math.pvar_t());
    b = coalesce(b, math.pvar_t());

    n = a.n + b.n;
    d = b.m1 - a.m1;
    a.m2 = a.m2 + b.m2 +  d * d * a.n * b.n / n;
    a.m1 = (a.m1 * a.n + b.m1 * b.n) / n;
    a.n = n;
    a.min = least(a.min, b.min);
    a.max = greatest(a.max, b.max);
    return a;
end;
$$;

drop operator if exists + (math.pvar_t, math.pvar_t);
create operator + (
    leftarg = math.pvar_t,
    rightarg = math.pvar_t,
    procedure = math.pvar,
    commutator = +
);

create or replace aggregate sum (
    math.pvar_t
)(
    sfunc = math.pvar, -- step-function
    stype = math.pvar_t
);


\if :test
    create function tests.test_pvar_parallel() returns setof text language plpgsql as $$
    declare
        x1 double precision[] = array[1,2,.3];
        x2 double precision[] = array[0,1,.5];
        x3 double precision[] = x1 || x2;

        a1 math.pvar_t;
        a2 math.pvar_t;
        a3 math.pvar_t;
        ok boolean = true;
        d double precision;
    begin
        select (math.pvar(x)).* into a1 from (select unnest(x1) x) t;
        select (math.pvar(x)).* into a2 from (select unnest(x2) x) t;
        select (math.pvar(x)).* into a3 from (select unnest(x3) x) t;

        return next ok(
            math.equ(math.var(a1 + a2), math.var(a3)),
            'pvar_t + pvar-t');

        return next ok(
            (select math.equ(math.var(sum(x)), math.var(a3))
            from (values (a1), (a2)) t (x)),
            'sum(parv_t)');
    end;
    $$;
\endif



-- ref:
-- https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance


