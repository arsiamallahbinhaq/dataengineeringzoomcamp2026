with source as (

    select *
    from {{ source('raw', 'fhv_tripdata') }}

),

renamed as (

    select
        -- identifiers
        dispatching_base_num,
        affiliated_base_number,

        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime

    from source

    -- Data quality filter
    where dispatching_base_num is not null

)

select *
from renamed

{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01'
  and pickup_datetime < '2019-02-01'
{% endif %}
