import os

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report


df = pd.read_csv("src/postvscomment/classification_dataset.csv")
df.head()

df["label"] = df["label"].map({"topic": 0, "message": 1})

X = df["text"]
y = df["label"]


X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)


vectorizer = TfidfVectorizer(stop_words="english")
X_train_vec = vectorizer.fit_transform(X_train)
X_test_vec = vectorizer.transform(X_test)

model = LogisticRegression(max_iter=1000)
model.fit(X_train_vec, y_train)

preds = model.predict(X_test_vec)
print("Accuracy:", accuracy_score(y_test, preds))
print(classification_report(y_test, preds))

feature_names = vectorizer.get_feature_names_out()
weights = model.coef_[0]

df_weights = pd.DataFrame({
    "word": feature_names,
    "weight": weights
})

# Add class interpretation
df_weights["class"] = df_weights["weight"].apply(
    lambda x: "message" if x > 0 else "topic"
)

# Sort by absolute importance
df_weights["abs_weight"] = df_weights["weight"].abs()
df_weights = df_weights.sort_values("abs_weight", ascending=False)

df_weights.head(20)
print(df_weights.head(10))

# support = 2
# There were 2 true POSTs in the test set

# recall = 1.00
# The model found both POSTs
# It did not miss any posts

# precision = 0.67
# The model predicted “POST” 3 times
# Only 2 of those were actually POST
# 2 / 3 ≈ 0.67