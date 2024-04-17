-- This macro is used to get a subquery with a where clause that can be used in a test
-- to filter the data to be tested. The macro looks for a where clause in the model's
-- config (schema.yml) and replaces the placeholder "__most_recent_year_month__" with
-- the maximum
-- year and month found in the relation. The macro returns a subquery with the where
-- thats used
-- to filter the data to be tested
{% macro get_where_subquery(relation) -%}
    {% set where = config.get("where", "") %}

    {% if where %}
        {% if "__most_recent_year_month__" in where %}
            {# Construct a query to find the maximum date using ano and mes columns #}
            {% set max_date_query = (
                "select format_date('%Y-%m', max(date(cast(ano as int64), cast(mes as int64), 1))) as max_date from "
                ~ relation
            ) %}
            {% set max_date_result = run_query(max_date_query) %}

            {% if execute %}
                {# % do log(max_date_query, info=True) %#}
                {# % do log(max_date_result, info=True) %#}
                {# Extract the maximum year and month from the max_date #}
                {% set max_date = max_date_result.rows[0][0] %}
                {% set max_year = max_date[:4] %}
                {% set max_month = max_date[5:7] %}

                {# Replace placeholder in the where config with actual maximum year and month #}
                {% set where = where | replace(
                    "__most_recent_year_month__",
                    "ano = " ~ max_year ~ " and mes = " ~ max_month,
                ) %}
                {% do log(
                    "----- The test will be performed for: " ~ where, info=True
                ) %}

            {% endif %}
        {% endif %}

        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}

        {% do return(filtered) %}
    {% else %} {% do return(relation) %}
    {% endif %}
{%- endmacro %}
