select distinct category_name eder from categories c 

SELECT DISTINCT Region
FROM Customers;

SELECT DISTINCT contact_title 
FROM Customers;

SELECT *
FROM Customers
ORDER BY Country;

SELECT *
FROM Orders
ORDER BY employee_id, order_date ;

SELECT *
FROM Customers
WHERE COALESCE(Region, '') = '';

SELECT product_name , COALESCE(unit_price, 10) AS Unit_Price
FROM Products;

SELECT Customers.company_name , Customers.contact_name , Orders.order_date 
FROM Customers
INNER JOIN Orders ON Customers.customer_id  = Orders.customer_id ;

SELECT od.order_id , p.product_name , od.Discount
FROM order_details od
INNER JOIN Products p ON od.product_id  = p.product_id ;

SELECT Customers.customer_id , Customers.company_name , Orders.order_id , Orders.order_id 
FROM Customers
LEFT JOIN Orders ON Customers.customer_id  = Orders.customer_id ;

SELECT et.employee_id, e.last_name, et.territory_id, t.territory_description
FROM employee_territories et
LEFT JOIN employees e ON et.employee_id = e.employee_id 
LEFT JOIN Territories t ON et.territory_id = t.territory_id;

SELECT Orders.order_id , Customers.company_name 
FROM Orders
LEFT JOIN Customers ON Orders.customer_id  = Customers.customer_id ;

SELECT Orders.order_id , Customers.company_name 
FROM Customers
RIGHT JOIN Orders ON Customers.customer_id  = Orders.customer_id ;

SELECT Customers.company_name, Orders.order_date 
FROM Customers
INNER JOIN Orders ON Customers.customer_id = Orders.customer_id 
INNER JOIN Shippers ON Orders.ship_via = Shippers.shipper_id 
WHERE EXTRACT(YEAR FROM Orders.order_date) = 1996;


SELECT e.first_name, e.last_name, et.territory_id 
FROM Employees e
FULL OUTER JOIN employee_territories et ON e.employee_id = et.employee_id;

SELECT 
c.category_name,
COALESCE(SUM(od.Quantity), 0) AS TotalQuantity,
COALESCE(SUM(od.unit_price * od.Quantity), 0) AS TotalSales
FROM 
Categories c
LEFT JOIN (
SELECT 
od.product_id,
od.Quantity,
od.Unit_Price,
p.Category_id
FROM 
Order_Details od
JOIN 
Products p ON od.product_id = p.product_id
JOIN 
Orders o ON od.order_id = o.order_id
) AS od ON c.category_id = od.category_id
GROUP BY 
c.category_name;


SELECT company_name  AS Nombre
FROM Customers
UNION
SELECT company_name  AS Nombre
FROM Suppliers;

SELECT E1.first_name || ' ' || E1.last_name AS Nombre
FROM employees E1
LEFT JOIN employees E2 ON E1.employee_id = E2.reports_to;

SELECT product_name 
FROM Products
WHERE product_id  IN (
    SELECT product_id 
    FROM order_details od 
);

SELECT company_name 
FROM Customers
WHERE customer_id  IN (
    SELECT DISTINCT customer_id 
    FROM Orders
    WHERE ship_country = 'Argentina'
);

SELECT product_name 
FROM Products
WHERE product_id NOT IN (
    SELECT DISTINCT product_id 
    FROM order_details od 
    WHERE order_id IN (
        SELECT order_id
        FROM Orders
        WHERE customer_id IN (
            SELECT customer_id
            FROM Customers
            WHERE Country = 'France'
        )
    )
);


SELECT order_id, SUM(quantity) AS Cantidad_Productos_Vendidos
FROM order_details od 
GROUP BY order_id ;


SELECT product_name , AVG(units_in_stock) AS Promedio_Productos_En_Stock
FROM Products
GROUP BY product_name ;

SELECT product_name , SUM(units_in_stock) AS Cantidad_Productos_En_Stock
FROM Products
GROUP BY product_name 
HAVING SUM(units_in_stock) > 100;

SELECT Customers.company_name , AVG(orders_per_company) AS Promedio_Pedidos
FROM (
    SELECT customer_id , COUNT(customer_id) AS orders_per_company
    FROM Orders
    GROUP BY customer_id 
) AS CompanyOrders
JOIN Customers ON CompanyOrders.customer_id = Customers.customer_id 
GROUP BY Customers.company_name 
HAVING AVG(orders_per_company) > 10;

SELECT
    Products.product_name ,
    CASE
        WHEN Products.Discontinued = 1 THEN 'Discontinued'
        ELSE Categories.category_name 
    END AS Category
FROM
    Products
    INNER JOIN Categories ON Products.category_id  = Categories.category_id ;

SELECT
    first_name || ' ' || last_name AS NombreEmpleado,
    CASE
        WHEN Title = 'Sales Manager' THEN 'Gerente de Ventas'
        ELSE Title
    END AS Titulo
FROM
    Employees;

-- EJERCICIOS SQL WIN FUNC
   -- 1

SELECT
    c.category_name,
    p.product_name,
    p.unit_price,
    AVG(p.unit_price) OVER(PARTITION BY c.category_name) ::numeric AS AvgPrice
FROM
    Products p
JOIN
    Categories c ON p.category_id = c.category_id;

 

--2

-- Consulta para obtener el promedio de venta de cada cliente

 

--consulta: pide el promedio de ventas por clientes y en este caso trae el cliente, el total de ordenes y el promedio de ventas

 
SELECT
    c.customer_id,
    o.order_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    COUNT(DISTINCT o.order_id) AS TotalOrders,
    AVG(od.unit_price * od.quantity) AS AvgSale
FROM
    customers c
INNER JOIN
    Orders o ON c.customer_id = o.customer_id
INNER JOIN
    order_details od ON o.order_id = od.order_id
GROUP BY
    c.customer_id, o.order_id, o.employee_id, o.order_date, o.required_date, o.shipped_date;


-- imagen#3
   
   SELECT
    AVG(od.unit_price * od.quantity) AS AverageOrderAmount,
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    (o.required_date - o.order_date) AS DaysBetweenOrderAndRequired
FROM
    Orders o
INNER JOIN
    order_details od ON o.order_id  = od.order_id 
GROUP BY
    o.order_id,
    o.customer_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    o.shipped_date;

-- #4
   
   SELECT

    customer_id,

    order_date,

    MIN(order_date) OVER(PARTITION BY customer_id) AS EarliestOrderDate

FROM

    Orders;
    
   
   -- 5) Consulta para seleccionar el ID de producto, el nombre de producto, el precio unitario, el ID de categoría y el precio unitario máximo para cada categoría

SELECT

    product_id,

    product_name,

    unit_price,

    category_id,

    MAX(unit_price) OVER(PARTITION BY category_id) AS MaxUnitPrice

FROM

    Products;

  --6) 
  WITH ProductSalesRank AS (
    SELECT
        p.product_id,
        p.product_name,
        SUM(od.quantity) AS TotalQuantitySold,
        ROW_NUMBER() OVER (ORDER BY SUM(od.quantity) DESC) AS SalesRank
    FROM
        Products p
    INNER JOIN
        order_details od ON p.product_id = od.product_id
    GROUP BY
        p.product_id, p.product_name
)
SELECT
    product_id,
    product_name,
    TotalQuantitySold,
    SalesRank
FROM
    ProductSalesRank
ORDER BY
    SalesRank;
   
   
 -- 7) Asignar numeros de fila para cada cliente, ordenados por customer_id

 SELECT
    ROW_NUMBER() OVER(ORDER BY customer_id) AS RowNumber,
    customer_id ,
    company_name ,
    contact_name ,
    Address
FROM
    Customers
ORDER BY
    customer_id;

   
   --8
   -- Consulta para obtener el ranking de los empleados más jóvenes

SELECT
    ROW_NUMBER() OVER(ORDER BY birth_date) AS Ranking,
    first_name || ' ' || last_name  AS EmployeeName,
    birth_date 
FROM
    employees
ORDER BY
    Ranking ASC;

   --9
   
   SELECT
    c.customer_id,
    c.company_name,
    SUM(od.unit_price * od.quantity) AS TotalSales
    FROM
    customers c
    INNER JOIN
    orders o ON c.customer_id = o.customer_id
    INNER JOIN
    order_details od ON o.order_id  = od.order_id 
    GROUP BY
    c.customer_id,
    c.company_name;
    
   --9 otra
   SELECT
    c.customer_id,
    c.company_name,
    o.order_id,
    o.employee_id,
    o.order_date,
    o.required_date,
    SUM(od.unit_price * od.quantity) AS TotalSales
FROM
    customers c
INNER JOIN
    orders o ON c.customer_id = o.customer_id
INNER JOIN
    order_details od ON o.order_id = od.order_id
GROUP BY
    c.customer_id,
    c.company_name,
    o.order_id,
    o.employee_id,
    o.order_date,
    o.required_date;

   
   --10

--Obtener la suma total de ventas por categoría de producto
   SELECT
    cat.category_id,
    cat.category_name,
    p.product_name,
    od.unit_price,
    od.quantity,
    SUM(od.unit_price * od.quantity) AS TotalSales
FROM
    Categories cat
INNER JOIN
    Products p ON cat.category_id = p.category_id
INNER JOIN
    order_details od ON p.product_id = od.product_id
GROUP BY
    cat.category_id,
    cat.category_name,
    p.product_name,
    od.unit_price,
    od.quantity;
-- otra opcion ejercicio 10 
  
WITH SalesByCategory AS (
    SELECT
        cat.category_id,
        cat.category_name,
        p.product_name,
        SUM(od.unit_price * od.quantity) AS TotalSales
    FROM
        Categories cat
    INNER JOIN
        Products p ON cat.category_id = p.category_id
    INNER JOIN
        order_details od ON p.product_id = od.product_id
    GROUP BY
        cat.category_id,
        cat.category_name,
        p.product_name
)
SELECT
    category_id,
    category_name,
    product_name,
    TotalSales,
    ROW_NUMBER() OVER (ORDER BY TotalSales DESC) AS Ranking
FROM
    SalesByCategory;
   
   
   --11

-- Consulta para calcular la suma total de gastos de envío por país de destino y ordenarlos por país en orden ascendente

SELECT
    ship_country,
    SUM(Freight) AS TotalFreight
FROM
    orders 
GROUP BY
    ship_country
ORDER BY
    ship_country  ASC;

   
   
  --12

--RANK////Ranking de ventas por cliente

SELECT
    c.customer_id,
    c.company_name,
    SUM(od.unit_price * od.quantity) AS TotalSales,
    RANK() OVER(ORDER BY SUM(od.unit_price * od.quantity) DESC) AS SalesRank

FROM
    customers c
INNER JOIN
    orders o ON c.customer_id = o.customer_id
INNER JOIN
    order_details od ON o.order_id = od.order_id
GROUP BY
    c.customer_id,
    c.company_name;

   
--13

-- Consulta para obtener el ranking de empleados por fecha de contratación

SELECT
    employee_id,
    first_name,
    last_name,
    hire_date,
    ROW_NUMBER() OVER(ORDER BY hire_date) AS HireDateRank
FROM

    employees;
   
   --14

-- Consulta para obtener el ranking de productos por precio unitario (desde el más barato al más caro)

SELECT
    product_id,
    product_name,
    unit_price,
    ROW_NUMBER() OVER(ORDER BY unit_price) AS PriceRank

FROM
    products;
   
   --15

--Mostrar por cada producto de una orden, la cantidad vendida y la cantidad

--vendida del producto previo.
SELECT
    order_id,
    product_id,
    quantity AS QuantitySold,
    LAG(quantity) OVER(PARTITION BY order_id ORDER BY product_id) AS PreviousQuantitySold
FROM
    order_details
ORDER BY
    order_id,
    product_id;
   
   
   --16)
WITH LastOrderDates AS (

    SELECT

        OrderID,

        CustomerID,

        OrderDate,

        LAG(OrderDate) OVER(PARTITION BY CustomerID ORDER BY OrderDate) AS LastOrderDate

    FROM

        Orders

)

 

SELECT

    OrderID,

    OrderDate,

    CustomerID,

    MAX(LastOrderDate) OVER(PARTITION BY CustomerID) AS LastOrderDate

FROM

    LastOrderDates;

 

--otra forma 16

SELECT
    order_id,
    order_date,
    customer_id,
    MAX(LastOrderDate) OVER(PARTITION BY customer_id) AS LastOrderDate

FROM (

    SELECT
        order_id,
        customer_id,
        order_date,

        LAG(order_date) OVER(PARTITION BY customer_id ORDER BY order_date) AS LastOrderDate
    FROM
        orders
) AS LastOrderDates;

--17
WITH ProductPrices AS (
    SELECT
        product_id,
        product_name,
        unit_price,
        LAG(unit_price) OVER(ORDER BY product_id) AS PreviousUnitPrice
    FROM

        products)

SELECT
    product_id,
    product_name,
    unit_price,
    PreviousUnitPrice,
    (unit_price - PreviousUnitPrice) AS PriceDifference

FROM
    ProductPrices;


--18
   WITH ProductPrices AS (

    select product_id, unit_price,
           LEAD(unit_price) OVER(ORDER BY product_id) AS NextUnitPrice
    from products )

SELECT
    product_id,
    unit_price AS CurrentPrice,
    NextUnitPrice AS NextPrice

FROM
    ProductPrices;
   
   

 
