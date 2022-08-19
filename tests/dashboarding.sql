

-- the goal is to summary of data as quickly possible
-- data are arranged as:


-- 1. most current/unprocessed data
--
create table tests.current (
    dev text not null,
    ts numeric,
    unique (dev, ts),

    val numeric
);

-- 2. contains prosessed "numranged"/slotted data
--
create extension if not exists btree_gist;

create table tests.slots (
    dev text not null,
    rng numrange,
    exclude using gist( dev with =, rng with &&),
    primary key (dev, rng), -- this is needed for upsert

    var math.pvar_t -- online statistics over rng period
);

-- 3. contains processed data for history / outside slot queries
--
create table tests.history (
) inherits (tests.current);



-- a sample values
insert into tests.current (dev, ts, val)
    values
    ('1', 100, 100),
    ('1', 101, 101),
    ('1', 170, 101),
    ('1', 190, 101),
    ('1', 200, 102),
    ('1', 201, 103),
    ('1', 300, 102),
    ('1', 301, 103),
    ('1', 1001, 1001),
    ('1', 1002, 1003);

-- supposed existing values
insert into tests.slots
values
    ('1', '[100,200)', null);


-- data is processed from the current
--
with
-- 1. for data < 1000
to_be_processed as (
    delete
    from tests.current
    where ts < 1000
    returning *
),

-- 2. slots are interval of 100
processed_slots as (
    select
        dev,
        numrange(floor(ts/100) * 100, (floor(ts/100)+1) * 100, '[)') as rng,
        math.pvar(val) as var
    from to_be_processed
    group by dev, rng
),

-- 3. slots are then updated
update_slots as (
    insert into tests.slots
    select *
    from processed_slots
    on conflict (dev, rng) do update
    set var = excluded.var
),

-- 4. stored into history table
insert_to_history_table as (
    insert into tests.history
    select *
    from to_be_processed
)
select null where false;


-- get values of arbritary range
--
create function tests.get_vars (
    p_rng numrange
)
    returns table (
        dev text,
        var math.pvar_t
    )
    language sql
    stable
as $$

    with
    -- 1. get the slot data
    --
    from_slots as (
        select *
        from tests.slots
        where p_rng @> rng
    ),
    min_max_range as (
        select
            dev,
            numrange(
                lower(p_rng),
                min(lower(rng))
            ) as historical_range,
            numrange (
                max(upper(rng)),
                upper(p_rng),
                '(]'
            ) as current_range
        from from_slots
        group by dev
    ),

    -- 2. get [min-range, min-slot) from history table
    --
    from_history as (
        select hist.dev, math.pvar(val) as pvar
        from tests.history hist
        join min_max_range mmr on mmr.dev = hist.dev
        where mmr.historical_range @> ts
        group by hist.dev
    ),

    -- 3. get (max-slot, max-range] only from current table
    --
    from_current as (
        select curr.dev, math.pvar(val) as pvar
        from only tests.current curr
        join min_max_range mmr on mmr.dev = curr.dev
        where mmr.current_range @> ts
        group by curr.dev
    )

    -- 4. combine to get overall union
    --
    select dev,
        sum(var)
    from (
        select dev, var from from_slots
        union all
        select * from from_current
        union all
        select * from from_history
    ) a
    group by dev;
$$;


create function tests.test_dashboarding() returns setof text
language plpgsql
as $$
declare
    a jsonb;
    b jsonb;
    nr numrange;
begin
    nr = numrange(150, 2000);
    select to_jsonb(t.*) into a from tests.get_vars( nr) t;
    select to_jsonb(t.*) into b from (
        select dev, math.pvar(val) as var
        from tests.current
        where ts between lower(nr) and upper(nr)
        group by dev
    ) t;

    return next ok (math.equ(
        math.var(jsonb_populate_record(null::math.pvar_t, a->'var')),
        math.var(jsonb_populate_record(null::math.pvar_t, b->'var'))
    ), 'equal 1');

    nr = numrange(3000, 4000);
    select to_jsonb(t.*) into a from tests.get_vars( nr ) t;
    select to_jsonb(t.*) into b from (
        select dev, math.pvar(val) as var
        from tests.current
        where ts between lower(nr) and upper(nr)
        group by dev
    ) t;
    return next ok (a is null and b is null, 'null as out of bound results');

end;
$$;