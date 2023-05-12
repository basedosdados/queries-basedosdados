from backend import Backend


if __name__ == "__main__":
    bk = Backend("https://staging.api.basedosdados.org/api/v1/graphql")
    r = bk._get_dataset_id_from_slug(dataset_slug="test_dataset")
    print(r)
    r = bk._get_table_id_from_slug(dataset_slug="test_dataset", table_slug="test_table")
    print(r)
    r = bk.get_dataset_config(dataset_id="test_dataset")
    print(r)
    r = bk.get_table_config(dataset_id="test_dataset", table_id="test_table")
    print(r)
