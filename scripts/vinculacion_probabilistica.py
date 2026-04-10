import splink
from splink.datasets import preprocessed_example_dataframe

def probabilistic_linkage(df1, df2):
    settings = {
        "link_type": "link_only",
        "blocking_rules_to_generate_predictions": [
            "l.first_name = r.first_name and l.surname = r.surname",
        ],
        "comparison_columns": [
            {
                "col_name": "first_name",
                "num_levels": 2,
                "case_expression": "col_lower",
            },
            {
                "col_name": "surname",
                "num_levels": 2,
                "case_expression": "col_lower",
            },
        ],
    }

    linker = splink.Splink(settings, [df1, df2])
    df_e = linker.get_scored_comparisons()
    linked_df = df_e[df_e["match_probability"] > 0.9]
    return linked_df
