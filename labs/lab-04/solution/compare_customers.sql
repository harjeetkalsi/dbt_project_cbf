{{ audit_helper.compare_relations(
    a_relation=ref('dim_customers_baseline'),
    b_relation=ref('dim_customers'),
    primary_key='customer_id'
) }}
