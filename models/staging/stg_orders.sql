with

source as (

    select * from {{ source('ecom', 'raw_orders') }}
    -- if you generate a larger dataset, you can limit the timespan to the current time with the following line
    -- where ordered_at <= {{ var('truncate_timespan_to') }}

),

renamed as (

    select

        ----------  ids
        id as order_id,
        store_id as location_id,
        customer as customer_id,

        ---------- numerics
        CAST(order_total / 100.0 as FLOAT64) as order_total,
        CAST(tax_paid / 100.0 as FLOAT64) as tax_paid,

        ---------- timestamps
        ordered_at

    from source

)

select * from renamed
