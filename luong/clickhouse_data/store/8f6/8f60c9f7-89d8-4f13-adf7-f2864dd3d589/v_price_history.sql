ATTACH VIEW _ UUID 'efba33c0-3516-43e5-8ec5-fd737f4261e2'
(
    `item_no` String,
    `product_name` String,
    `store_code` UInt32,
    `change_type` String,
    `valid_from` Date,
    `valid_to` Nullable(Date),
    `is_current` UInt8,
    `selling_price` Nullable(Float64),
    `original_price` Nullable(Float64),
    `is_promo` Nullable(UInt8),
    `promotion_code` Nullable(String)
)
AS SELECT
    h.item_no,
    p.product_name,
    h.store_code,
    h.change_type,
    h.valid_from,
    if(h.next_from = toDate(0), NULL, h.next_from) AS valid_to,
    h.next_from = toDate(0) AS is_current,
    h.selling_price,
    h.original_price,
    h.is_promo,
    h.promotion_code
FROM
(
    SELECT
        item_no,
        store_code,
        op,
        change_type,
        YYYYMMDDToDate(change_date_id) AS valid_from,
        new_selling_price AS selling_price,
        new_original_price AS original_price,
        new_is_promo AS is_promo,
        new_promotion_code AS promotion_code,
        leadInFrame(YYYYMMDDToDate(change_date_id)) OVER (PARTITION BY item_no, store_code ORDER BY change_date_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_from
    FROM winmart.price_changes
) AS h
LEFT JOIN
(
    SELECT
        item_no,
        product_name
    FROM winmart.dim_product
    FINAL
) AS p ON h.item_no = p.item_no
WHERE h.op != 'D'
