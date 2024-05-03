-- o objetivo do teste é verificar se todos os valores que estão presentes em colunas
-- de tabelas com dicionário também estão nos dicionários
{% test custom_dictionaries(
    model, table_id, dictionary_model_name, columns_covered_by_dictionary
) %}

    {{ config(severity="error") }}

    {% for column_name in columns_covered_by_dictionary %}

        with
            left_table as (
                select {{ column_name }} as id
                from {{ model }}
                where {{ column_name }} is not null
            ),

            right_table as (
                select chave as id
                from {{ dictionary_model_name }}
                where
                    valor is not null
                    and id_tabela = '{{ table_id }}'
                    and nome_coluna = '{{ column_name }}'
            ),

            exceptions as (
                select left_table.id
                from left_table
                left join right_table on left_table.id = right_table.id
                where right_table.id is null
            )

        select '{{ column_name }}' as failed_column, id as missing_value
        from exceptions
        {% if not loop.last %}
            union all
        {% endif %}

    {% endfor %}

{% endtest %}
