```sql
{% macro check_schema_changes() %}

    {% set run_id = invocation_id %}

    {% set tables = {

        'VEHICLE_SALES': {
            'SALE_ID': 'VARCHAR',
            'VIN': 'VARCHAR',
            'DEALER_ID': 'VARCHAR',
            'SALE_DATE': 'DATE',
            'SALE_AMOUNT': 'NUMBER',
            'CURRENCY': 'VARCHAR',
            'SALES_CHANNEL': 'VARCHAR',
            'STATUS': 'VARCHAR',
            'UPDATED_AT': 'TIMESTAMP_NTZ',
            'SOURCE_SYSTEM': 'VARCHAR',
            'SOURCE_FILE_NAME': 'VARCHAR',
            'INGESTION_TIMESTAMP': 'TIMESTAMP_NTZ',
            'RUN_ID': 'VARCHAR'
        },

        'VEHICLES': {
            'VIN': 'VARCHAR',
            'MODEL': 'VARCHAR',
            'MODEL_YEAR': 'NUMBER',
            'FUEL_TYPE': 'VARCHAR',
            'PLANT_CODE': 'VARCHAR',
            'UPDATED_AT': 'TIMESTAMP_NTZ',
            'SOURCE_SYSTEM': 'VARCHAR',
            'SOURCE_FILE_NAME': 'VARCHAR',
            'INGESTION_TIMESTAMP': 'TIMESTAMP_NTZ',
            'RUN_ID': 'VARCHAR'
        },

        'DEALERS': {
            'DEALER_ID': 'VARCHAR',
            'DEALER_NAME': 'VARCHAR',
            'CITY': 'VARCHAR',
            'STATE': 'VARCHAR',
            'REGION': 'VARCHAR',
            'UPDATED_AT': 'TIMESTAMP_NTZ',
            'SOURCE_SYSTEM': 'VARCHAR',
            'SOURCE_FILE_NAME': 'VARCHAR',
            'INGESTION_TIMESTAMP': 'TIMESTAMP_NTZ',
            'RUN_ID': 'VARCHAR'
        }

    } %}


    {% for table_name, expected_schema in tables.items() %}

        {{ log('========================================', info=True) }}
        {{ log('CHECKING TABLE: RAW.' ~ table_name, info=True) }}
        {{ log('========================================', info=True) }}


        {% set query %}

            SELECT
                UPPER(COLUMN_NAME) AS COLUMN_NAME,
                UPPER(DATA_TYPE) AS DATA_TYPE

            FROM AUTOMOTIVE_DBT_DB.INFORMATION_SCHEMA.COLUMNS

            WHERE TABLE_SCHEMA = 'RAW'
              AND TABLE_NAME = '{{ table_name }}'

            ORDER BY ORDINAL_POSITION

        {% endset %}


        {% set actual_schema = run_query(query) %}


        {% if execute %}

            {% set actual_columns = {} %}


            {# =========================================================
               READ ACTUAL SNOWFLAKE SCHEMA
               ========================================================= #}

            {% for row in actual_schema.rows %}

                {% set actual_type = row['DATA_TYPE'] | upper %}

                {% if actual_type in ['TEXT', 'STRING', 'VARCHAR'] %}

                    {% set normalized_type = 'VARCHAR' %}

                {% else %}

                    {% set normalized_type = actual_type %}

                {% endif %}

                {% do actual_columns.update({
                    row['COLUMN_NAME']: normalized_type
                }) %}

            {% endfor %}


            {# =========================================================
               CHECK EXPECTED COLUMNS
               MISSING COLUMN + DATA TYPE CHANGE
               ========================================================= #}

            {% for column, expected_type in expected_schema.items() %}

                {% set expected_type_normalized = expected_type | upper %}


                {% if expected_type_normalized in ['TEXT', 'STRING', 'VARCHAR'] %}

                    {% set expected_type_normalized = 'VARCHAR' %}

                {% endif %}


                {# =====================================================
                   MISSING COLUMN
                   CRITICAL - STOP
                   ===================================================== #}

                {% if column not in actual_columns %}

                    {{ log(
                        'MISSING COLUMN: ' ~ column,
                        info=True
                    ) }}


                    {% set audit_sql %}

                        INSERT INTO AUTOMOTIVE_DBT_DB.AUDIT.SCHEMA_CHANGE_AUDIT
                        (
                            RUN_ID,
                            TABLE_NAME,
                            CHANGE_TYPE,
                            COLUMN_NAME,
                            EXPECTED_TYPE,
                            ACTUAL_TYPE,
                            STATUS,
                            SEVERITY
                        )
                        VALUES
                        (
                            '{{ run_id }}',
                            'RAW.{{ table_name }}',
                            'MISSING_COLUMN',
                            '{{ column }}',
                            '{{ expected_type_normalized }}',
                            NULL,
                            'DETECTED',
                            'CRITICAL'
                        )

                    {% endset %}


                    {% do run_query(audit_sql) %}


                    {{ exceptions.raise_compiler_error(
                        'CRITICAL SCHEMA CHANGE: MISSING COLUMN '
                        ~ column
                        ~ ' detected in RAW.'
                        ~ table_name
                        ~ '. Pipeline stopped.'
                    ) }}


                {# =====================================================
                   DATA TYPE CHANGE
                   CRITICAL - STOP
                   ===================================================== #}

                {% elif actual_columns[column] != expected_type_normalized %}

                    {{ log(
                        'DATA TYPE CHANGE: '
                        ~ column
                        ~ ' | Expected=' ~ expected_type_normalized
                        ~ ' | Actual=' ~ actual_columns[column],
                        info=True
                    ) }}


                    {% set audit_sql %}

                        INSERT INTO AUTOMOTIVE_DBT_DB.AUDIT.SCHEMA_CHANGE_AUDIT
                        (
                            RUN_ID,
                            TABLE_NAME,
                            CHANGE_TYPE,
                            COLUMN_NAME,
                            EXPECTED_TYPE,
                            ACTUAL_TYPE,
                            STATUS,
                            SEVERITY
                        )
                        VALUES
                        (
                            '{{ run_id }}',
                            'RAW.{{ table_name }}',
                            'DATA_TYPE_CHANGE',
                            '{{ column }}',
                            '{{ expected_type_normalized }}',
                            '{{ actual_columns[column] }}',
                            'DETECTED',
                            'CRITICAL'
                        )

                    {% endset %}


                    {% do run_query(audit_sql) %}


                    {{ exceptions.raise_compiler_error(
                        'CRITICAL SCHEMA CHANGE: DATA TYPE CHANGE for '
                        ~ column
                        ~ ' detected in RAW.'
                        ~ table_name
                        ~ '. Pipeline stopped.'
                    ) }}


                {% endif %}

            {% endfor %}


            {# =========================================================
               CHECK NEW COLUMNS
               CRITICAL - STOP
               ========================================================= #}

            {% for column, actual_type in actual_columns.items() %}

                {% if column not in expected_schema %}

                    {{ log(
                        'NEW COLUMN: '
                        ~ column
                        ~ ' | Type=' ~ actual_type,
                        info=True
                    ) }}


                    {% set audit_sql %}

                        INSERT INTO AUTOMOTIVE_DBT_DB.AUDIT.SCHEMA_CHANGE_AUDIT
                        (
                            RUN_ID,
                            TABLE_NAME,
                            CHANGE_TYPE,
                            COLUMN_NAME,
                            EXPECTED_TYPE,
                            ACTUAL_TYPE,
                            STATUS,
                            SEVERITY
                        )
                        VALUES
                        (
                            '{{ run_id }}',
                            'RAW.{{ table_name }}',
                            'NEW_COLUMN',
                            '{{ column }}',
                            NULL,
                            '{{ actual_type }}',
                            'DETECTED',
                            'CRITICAL'
                        )

                    {% endset %}


                    {# First save the event to audit table #}

                    {% do run_query(audit_sql) %}


                    {# Then stop dbt execution #}

                    {{ exceptions.raise_compiler_error(
                        'CRITICAL SCHEMA CHANGE: NEW COLUMN '
                        ~ column
                        ~ ' detected in RAW.'
                        ~ table_name
                        ~ '. Pipeline stopped.'
                    ) }}


                {% endif %}

            {% endfor %}


            {{ log(
                'Schema check completed for RAW.' ~ table_name,
                info=True
            ) }}

        {% endif %}

    {% endfor %}

{% endmacro %}
```
