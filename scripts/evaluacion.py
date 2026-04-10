def evaluate_linkage(linked_df, true_links):
    # Compara linked_df con true_links para calcular falsos positivos y negativos
    true_positives = linked_df[linked_df['id'].isin(true_links['id'])]
    false_positives = linked_df[~linked_df['id'].isin(true_links['id'])]
    false_negatives = true_links[~true_links['id'].isin(linked_df['id'])]
    
    return true_positives, false_positives, false_negatives
