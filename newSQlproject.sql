USE world;
SELECT *
FROM messy_customer_orders;


CREATE TABLE new_customer_data
LIKE messy_customer_orders;

SELECT *
FROM new_customer_data;

INSERT new_customer_data
SELECT *
FROM messy_customer_orders;


SELECT *
FROM new_customer_data;

----- #removing_duplicates 

WITH duplicate_data AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY order_id, customer_id, customer_name, email, country, city, signup_date, 
order_date, product_category, product_name,
quantity, unit_price, discount_percent, payment_method, delivery_status) AS row_num
FROM new_customer_data
)

	SELECT *
    FROM duplicate_data
	WHERE row_num >= 2;
    
    
DROP TABLE IF EXISTS new_customer_data2;
CREATE TABLE `new_customer_data2` (
  `order_id` text,
  `customer_id` text,
  `customer_name` text,
  `email` text,
  `country` text,
  `city` text,
  `signup_date` text,
  `order_date` text,
  `product_category` text,
  `product_name` text,
  `quantity` int DEFAULT NULL,
  `unit_price` double DEFAULT NULL,
  `discount_percent` text,
  `payment_method` text,
  `delivery_status` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM new_customer_data2;


INSERT INTO new_customer_data2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY order_id, customer_id, customer_name, email, country, city, signup_date, 
order_date, product_category, product_name,
quantity, unit_price, discount_percent, payment_method, delivery_status) AS row_num
FROM new_customer_data;




SET SQL_SAFE_UPDATES = 0;
DELETE
FROM new_customer_data2
WHERE row_num > 1; 



----cleaning customer_names


ALTER TABLE new_customer_data
ADD COLUMN cleaned_customer_name VARCHAR(250);


USE world;

UPDATE new_customer_data
SET cleaned_customer_name = CONCAT(
	UPPER(LEFT(SUBSTRING_INDEX(customer_name, ' ', 1), 1)),
	LOWER(SUBSTRING(SUBSTRING_INDEX(customer_name, ' ', 1), 2)), 
	' ',
	UPPER(LEFT(SUBSTRING_INDEX(customer_name, ' ', -1), 1)),
	LOWER(SUBSTRING(SUBSTRING_INDEX(customer_name, ' ', -1), 2))
) 
WHERE customer_name IS NOT NULL;


------cleaning city_names

ALTER TABLE new_customer_data
ADD COLUMN cleaned_city VARCHAR(250);


UPDATE new_customer_data
SET cleaned_city = CASE 
		WHEN city LIKE '% %' THEN
CONCAT(
UPPER(LEFT(SUBSTRING_INDEX(city, ' ', 1), 1)),
LOWER(SUBSTRING(SUBSTRING_INDEX(city, ' ', 1), 2)), 
' ',
UPPER(LEFT(SUBSTRING_INDEX(city, ' ', -1), 1)),
LOWER(SUBSTRING(SUBSTRING_INDEX(city, ' ', -1), 2))
) 
ELSE 
	CONCAT(
		UPPER(LEFT(city, 1)),
		LOWER(SUBSTRING(city, 2))
        )
        END
WHERE city IS NOT NULL;


------- cleaning email
ALTER TABLE new_customer_data
ADD COLUMN cleaned_email VARCHAR(250);


UPDATE new_customer_data
SET cleaned_email = LOWER(email) 
WHERE email IS NOT NULL;


----- cleaning_payment_method
ALTER TABLE new_customer_data
ADD COLUMN cleaned_payment_method VARCHAR(100);

UPDATE new_customer_data
SET cleaned_payment_method = CASE
WHEN payment_method LIKE '% %' THEN
CONCAT(
UPPER(LEFT(SUBSTRING_INDEX(payment_method, ' ', 1), 1)),
LOWER(SUBSTRING(SUBSTRING_INDEX(payment_method, ' ', 1), 2)), 
' ',
UPPER(LEFT(SUBSTRING_INDEX(payment_method, ' ', -1), 1)),
LOWER(SUBSTRING(SUBSTRING_INDEX(payment_method, ' ', -1), 2))
) 
ELSE 
	CONCAT(
		UPPER(LEFT(payment_method, 1)),
		LOWER(SUBSTRING(payment_method, 2))
        )
        END
WHERE payment_method IS NOT NULL;


SELECT *
FROM new_customer_data;

---- cleaning order_date


ALTER TABLE new_customer_data 
ADD COLUMN cleaned_order_date DATE;

UPDATE new_customer_data
SET cleaned_order_date = CASE

    WHEN LENGTH(SUBSTRING_INDEX(order_date, '/', -1)) = 2 
        THEN STR_TO_DATE(order_date, '%d/%m/%y')

    WHEN LENGTH(SUBSTRING_INDEX(order_date, '/', -1)) = 4 
        THEN STR_TO_DATE(order_date, '%d/%m/%Y')
    ELSE NULL
END;




SELECT cleaned_order_date
FROM new_customer_data;

------ cleaning_signup_date

ALTER TABLE new_customer_data 
ADD COLUMN cleaned_signup_date DATE;

UPDATE new_customer_data
SET cleaned_signup_date = CASE
    WHEN LENGTH(SUBSTRING_INDEX(signup_date, '/', -1)) = 2 
        THEN STR_TO_DATE(signup_date, '%d/%m/%y')
        
    WHEN LENGTH(SUBSTRING_INDEX(signup_date, '/', -1)) = 4 
        THEN STR_TO_DATE(signup_date, '%d/%m/%Y')
    ELSE NULL
END;


----- cleaning delivery_status
 
ALTER TABLE new_customer_data
ADD COLUMN cleaned_delivery_status VARCHAR(255);

UPDATE new_customer_data
SET cleaned_delivery_status = CASE
	WHEN LOWER(delivery_status) IN ('deliver', 'Delivered') THEN 'Delivered'
	WHEN LOWER(delivery_status) = 'pending' THEN 'Pending'
	WHEN LOWER(delivery_status) = 'cancelled' THEN 'Cancelled'
    ELSE CONCAT(
			UPPER(LEFT(LOWER(delivery_status), 1)),
        LOWER(SUBSTRING(LOWER(delivery_status), 2))
    )
END;
    
    
-----cleaning_discount_percent

ALTER TABLE new_customer_data
ADD COLUMN cleaned_discount_percent DECIMAL(5,2);

UPDATE new_customer_data
SET cleaned_discount_percent = CAST(
    NULLIF(REPLACE(REPLACE(discount_percent, '%', ''), ',', ''), 'N/A') 
    AS DECIMAL(5,2)
);



DROP TABLE IF EXISTS new_cleaned_data;
CREATE TABLE `new_cleaned_data` (
  `order_id` VARCHAR(50),
  `customer_id` VARCHAR(50),
  `customer_name` VARCHAR(100),
  `email` VARCHAR(255),
  `country` VARCHAR(100),
  `city` VARCHAR(100),
  `signup_date` DATE,
  `order_date` DATE,
  `product_category` VARCHAR(100),
  `product_name` VARCHAR(100),
  `quantity` INT DEFAULT NULL,
  `unit_price` DECIMAL(10,2) DEFAULT NULL,
  `discount_percent` VARCHAR(255),
  `payment_method` TEXT(100),
  `delivery_status` VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO new_cleaned_data (
	order_id,
	customer_id,
	customer_name,
	email,
	country,
	city,
	signup_date,
	order_date,
	product_category,
	product_name,
	quantity,
	unit_price,
	discount_percent,
	payment_method,
	delivery_status
)
SELECT 
	order_id,
	customer_id,
    cleaned_customer_name,
    cleaned_email,
    country,
	cleaned_city,
    cleaned_signup_date,
	cleaned_order_date,
	product_category,
	product_name,
	quantity,
	unit_price,
	 cleaned_discount_percent,
    cleaned_payment_method,
	cleaned_delivery_status
FROM new_customer_data


SELECT *
FROM new_cleaned_data;
