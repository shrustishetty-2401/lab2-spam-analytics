{% snapshot spam_emails_snapshot %}

{{
    config(
      target_schema='PUBLIC',
      unique_key='EMAIL_ID',
      strategy='timestamp',
      updated_at='LOADED_AT'
    )
}}

SELECT * FROM {{ source('public', 'RAW_SPAM_EMAILS') }}

{% endsnapshot %}
