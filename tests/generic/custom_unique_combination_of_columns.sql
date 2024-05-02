{% test custom_unique_combinations_of_columns(
    model, combination_of_columns, proportion_allowed_failures=5
) %}

    {{ config(severity="error") }}

    {%- set column_list = combination_of_columns %}
    {%- set columns_csv = column_list | join(", ") %}

    with
        validation_data as (
            select {{ columns_csv }}, count(*) as duplicates_count
            from {{ model }}
            group by {{ columns_csv }}
            having count(*) > 1
        ),
        summary as (
            select duplicates_count, (select count(*) from {{ model }}) as total_rows
            from validation_data
        ),

        final_summary as (
            select
                duplicates_count,
                total_rows,
                round(((duplicates_count) / total_rows), 2) as proportion
            from summary
        )

    select
        duplicates_count,
        total_rows,
        proportion,
        case
            when proportion > {{ proportion_allowed_failures }}
            then
                'Test failed: Proportion of non-unique '
                || proportion
                || '% exceeds allowed proportion '
                || '{{ proportion_allowed_failures }}%'
            else
                'Test passed: Proportion of non-unique '
                || proportion
                || '% within acceptable limits'
        end as log_message
    from final_summary
    where proportion > {{ proportion_allowed_failures }}

{% endtest %}
