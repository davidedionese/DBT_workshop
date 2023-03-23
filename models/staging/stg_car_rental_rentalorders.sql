WITH 

rentalorders AS (

    SELECT * FROM {{ source('car_rental', 'rentalorders') }}

),

renamed AS (

    SELECT

        rentalorders.rentalorderid AS rentalorder_id,
        rentalorders.customerid AS customer_id,
        rentalorders.carid AS car_id,
        rentalorders.orderstatus AS order_status,
        rentalorders.mileagestart AS mileage_start,
        rentalorders.mileageend AS mileage_end

    FROM rentalorders

)

SELECT * FROM renamed