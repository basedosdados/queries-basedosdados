{% test not_null_proportion_multiple_columns(model, at_least=0.05) %}

    {%- set columns = adapter.get_columns_in_relation(model) -%}
    {% set suffix = "_nulls" %}
    {% set pivot_columns_query %}

        with null_counts as(

            select
                {% for column in columns -%}
                SUM(CASE WHEN {{ column.name }} IS NULL THEN 1 ELSE 0 END) AS {{ column.name }}{{ suffix }},
                {%- endfor %}
                count(*) as total_records
                from {{ model }}
        ),

        pivot_columns as (

            {% for column in columns -%}
            select '{{ column.name }}' as column_name, {{ column.name }}{{ suffix }} as quantity, total_records
            from null_counts
            {% if not loop.last %}union all {% endif %}
            {%- endfor %}
        ),

        faulty_columns as (
            select
                *
            from pivot_columns
            where
                quantity / total_records > (1 - {{ at_least }})


        )
        select * from faulty_columns
    {% endset %}
    with
        validation_errors as (
            {%- set errors = dbt_utils.get_query_results_as_dict(
                pivot_columns_query
            ) -%}
    {% if errors["column_name"] != () %}
                {% for e in errors["column_name"] | unique %}
                    {{
                        log(
                            "LOG: Coluna com preenchimento menor que "
                            ~ at_least * 100
                            ~ "% ---> "
                            ~ e
                            ~ "  [FAIL]",
                            info=True,
                        )
                    }}
                    select '{{e}}' as column
                    {% if not loop.last %}
                        union all
                    {% endif %}
                {% endfor %}
            )
        select *
        from validation_errors
    {% else %}select 1 as column) select * from validation_errors where column != 1
    {% endif %}

{% endtest %}
