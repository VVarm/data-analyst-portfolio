-- ------------------------------------------------------------
-- ЗАПРОС 1: Расшифровка массива товаров в заказе (JOIN + unnest)
-- ------------------------------------------------------------
-- Задача: В таблице orders товары хранятся в виде массива ID [1, 5, 10].
-- Нужно для каждого заказа получить читаемые названия товаров через запятую.
-- Сложность: Работа с вложенными структурами и двойная агрегация.

SELECT 
    order_id,
    array_agg(name) AS product_names   -- собираем названия обратно в массив
FROM (
    -- Подзапрос: сопоставляем ID товара с его названием через JOIN
    SELECT 
        order_id,
        name
    FROM (
        -- Разворачиваем массив product_ids в отдельные строки
        SELECT 
            order_id,
            unnest(product_ids) AS product_id
        FROM orders
    ) AS order_items
    INNER JOIN products USING (product_id)  -- подтягиваем название товара
) AS order_with_names
GROUP BY order_id
LIMIT 1000;

-- ------------------------------------------------------------
-- ЗАПРОС 2: Сегментация заказов по размеру (CASE + GROUP BY)
-- ------------------------------------------------------------
-- Задача: Разбить заказы на три категории:
--   - Малый: 1-3 товара
--   - Средний: 4-6 товаров
--   - Большой: 7 и более товаров
-- Посчитать количество заказов в каждой категории.

SELECT 
    CASE 
        WHEN array_length(product_ids, 1) <= 3 THEN 'Малый'
        WHEN array_length(product_ids, 1) BETWEEN 4 AND 6 THEN 'Средний'
        ELSE 'Большой'
    END AS order_size,
    COUNT(order_id) AS orders_count
FROM orders
GROUP BY order_size
ORDER BY orders_count DESC;  -- сортируем по убыванию, чтобы видеть самых популярных

-- ------------------------------------------------------------
-- ЗАПРОС 3: Накопительная воронка и процент отмен (Оконные функции)
-- ------------------------------------------------------------
-- Задача: Отследить в хронологическом порядке для каждого пользователя,
-- сколько заказов он создал, сколько отменил и какой текущий % отмен.
-- Это позволяет в реальном времени выявлять пользователей с аномально высоким
-- процентом отказов или технические проблемы на этапе оформления.

SELECT 
    user_id,
    order_id,
    action,
    time,
    -- Накопительное количество созданных заказов (для данного пользователя)
    SUM(created_type::INTEGER) OVER(
        PARTITION BY user_id 
        ORDER BY time 
        ROWS UNBOUNDED PRECEDING
    ) AS created_orders,
    
    -- Накопительное количество отменённых заказов
    SUM(canceled_type::INTEGER) OVER(
        PARTITION BY user_id 
        ORDER BY time 
        ROWS UNBOUNDED PRECEDING
    ) AS canceled_orders,
    
    -- Расчёт текущего процента отмен (Cancel Rate)
    ROUND(
        SUM(canceled_type::INTEGER) OVER(
            PARTITION BY user_id 
            ORDER BY time 
            ROWS UNBOUNDED PRECEDING
        )::DECIMAL / 
        SUM(created_type::INTEGER) OVER(
            PARTITION BY user_id 
            ORDER BY time 
            ROWS UNBOUNDED PRECEDING
        ),
        2
    ) AS cancel_rate
FROM (
    -- Подготовка данных: превращаем действия в числовые флаги (1 или 0)
    SELECT 
        user_id,
        order_id,
        action,
        time,
        CASE WHEN action = 'create_order' THEN true ELSE false END AS created_type,
        CASE WHEN action = 'cancel_order' THEN true ELSE false END AS canceled_type
    FROM user_actions
) AS t
ORDER BY user_id, order_id, time
LIMIT 1000;
