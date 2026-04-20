{{ config(
    materialized='view',
    pre_hook="USE WAREHOUSE RABBIT_QUERY_WH"
) }}

SELECT
    EMAIL_ID,
    LABEL,
    MESSAGE,
    CASE WHEN LABEL = 'spam' THEN 1 ELSE 0 END AS IS_SPAM,
    LENGTH(MESSAGE) AS MESSAGE_LENGTH,
    ARRAY_SIZE(SPLIT(MESSAGE, ' ')) AS WORD_COUNT,
    LOADED_AT
FROM {{ source('public', 'RAW_SPAM_EMAILS') }}
