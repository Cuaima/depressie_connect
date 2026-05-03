import pandas as pd
import sys
import os

# Ensure CDS.py can be imported
sys.path.append(os.path.dirname(__file__))

from CDS import process_dataset

# ---------------------------
# SAMPLE DATASET (TEST ONLY)
# ---------------------------
tweets = pd.DataFrame({
    "text": [
        "ik voel me altijd waardeloos en alles gaat fout",
        "niemand luistert ooit naar mij en het zal nooit beter worden",
        "ik denk dat mensen me haten",
        "vandaag ging het iets beter en ik heb gewandeld",
        "ik ben een mislukkeling en ik zal altijd zo blijven"
    ]
})

# REQUIRED: lowercase text
tweets["text"] = tweets["text"].str.lower()

# ---------------------------
# RUN CDS
# ---------------------------
cds_per_tweet = process_dataset(
    tweets,
    output="per_tweet",
    language="NL"
)

cds_per_category = process_dataset(
    tweets,
    output="per_category",
    language="NL"
)

print("\n=== Per tweet ===")
print(cds_per_tweet)

# Show only CDS categories that actually occur
active_categories = cds_per_category.loc[:, cds_per_category.sum() > 0]

print("\n=== Per category (only active categories) ===")
print(active_categories)

