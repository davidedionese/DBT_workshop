with rentalorders AS (

    SELECT * FROM {{ ref('stg_car_rental_rentalorders') }}

),
cars as 
(
    select
        cars.carid as car_id,
        cars.make,
        cars.model,
        cars.color
        from {{ source('car_rental', 'cars')}} as cars
),
rentalcategories as
(
    select
        rentalcategories.rentalcategoriesid as rentalcategories_id,
        rentalcategories.category,
        case when round(rentalcategories.monthlyrentprice,2) = 300.0 then 'car category 1'
        when round(rentalcategories.monthlyrentprice,2) = 400.0 then 'car category 2'
        when round(rentalcategories.monthlyrentprice,2) = 500.0 then 'car category 3'
        else 'car category 4'
        end as car_category,
        round(rentalcategories.monthlyrentprice,2) as monthly_rent_price
    from {{ source('car_rental', 'rentalcategories')}} as rentalcategories
),
final as 
(
    select
        customers.customerid as customer_id,
        customers.fullname,
        rentalcategories.category as customer_category,
        rentalcategories.monthly_rent_price,
        cars.make,
        cars.model,
        cars.color,
        rentalcategories.car_category
    from {{ source('car_rental', 'customers')}} as customers

    inner join rentalorders
        on customers.customerid = rentalorders.customer_id

    inner join cars
        on rentalorders.car_id = cars.car_id
    inner join rentalcategories 
        on customers.rentalcategoriesid = rentalcategories.rentalcategories_id
)

select * from final