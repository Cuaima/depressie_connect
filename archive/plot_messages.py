import pandas as pd
import matplotlib.pyplot as plt

# laad je merged dataset
df = pd.read_csv("Data2/merged-untilMarch2026.csv", sep=";")

# alleen echte berichten
df = df[df["post_type"] == "Message"]

# datum omzetten naar datetime
df["PostDate"] = pd.to_datetime(df["PostDate"], errors="coerce")

# jaar eruit halen
df["year"] = df["PostDate"].dt.year

# aantal berichten per jaar tellen
messages_per_year = df.groupby("year").size()

# plot maken
plt.figure()
messages_per_year.plot(kind="line", marker="o")

plt.xlabel("Year")
plt.ylabel("Number of messages")
plt.title("Messages per year")

plt.show()