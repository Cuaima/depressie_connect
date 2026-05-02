python -c "
import pandas as pd

# Load old groups to get account type per group
groups = pd.read_csv('data/groups.csv')
print('Old groups columns:', groups.columns.tolist())

# Load exact overlaps
overlaps = pd.read_csv('output/diag_exact_overlaps.csv')

# Load old messages to get ForumGroupID per topic
messages = pd.read_csv('data/messages.csv')
topics   = pd.read_csv('data/topics.csv')

# Join topics → groups → account type
topic_group = topics[['ForumTopicID','ForumGroupID']].merge(
    groups[['ForumGroupID','AccountID','Name']], on='ForumGroupID', how='left'
)

# Attach account type to overlapping old messages
overlaps_with_account = overlaps.merge(
    topic_group.rename(columns={'ForumTopicID':'ForumTopicID_old'}),
    on='ForumTopicID_old', how='left'
)

print(overlaps_with_account[['PosterID_old','PosterID_new','AccountID','Name']].value_counts(['PosterID_new','AccountID','Name']).head(20).to_string())
"