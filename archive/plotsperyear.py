import pandas as pd
import matplotlib.pyplot as plt

# load data
df = pd.read_csv("Data2/merged-untilMarch2026.csv", sep=";")

# alleen echte berichten
df = df[df["post_type"] == "Message"]

# datum goed zetten
df["PostDate"] = pd.to_datetime(df["PostDate"], errors="coerce")
df["year"] = df["PostDate"].dt.year

# =========================
# 📊 BASIS METRICS
# =========================

messages_per_year = df.groupby("year").size()
users_per_year = df.groupby("year")["AuthorID"].nunique()
topics_per_year = df.groupby("year")["ForumTopicID"].nunique()

messages_per_user = messages_per_year / users_per_year
messages_per_topic = messages_per_year / topics_per_year

# =========================
# 📈 PRINTS (snelle inzichten)
# =========================

print("\n📊 Messages per year:\n", messages_per_year)
print("\n👤 Users per year:\n", users_per_year)
print("\n💬 Messages per user:\n", messages_per_user)
print("\n🧵 Topics per year:\n", topics_per_year)
print("\n📈 Messages per topic:\n", messages_per_topic)

# =========================
# 🔍 TOP USERS (2024)
# =========================

top_users_2024 = df[df["year"] == 2024]["AuthorID"].value_counts().head(10)

print("\n🔥 Top users in 2024:\n", top_users_2024)

# =========================
# 📊 PLOTS
# =========================

plt.figure()
messages_per_year.plot(marker="o")
plt.title("Messages per year")
plt.xlabel("Year")
plt.ylabel("Number of messages")

plt.figure()
users_per_year.plot(marker="o")
plt.title("Unique users per year")
plt.xlabel("Year")
plt.ylabel("Number of users")

plt.figure()
messages_per_user.plot(marker="o")
plt.title("Messages per user")
plt.xlabel("Year")
plt.ylabel("Messages per user")

plt.figure()
topics_per_year.plot(marker="o")
plt.title("Topics per year")
plt.xlabel("Year")
plt.ylabel("Number of topics")

plt.figure()
messages_per_topic.plot(marker="o")
plt.title("Messages per topic")
plt.xlabel("Year")
plt.ylabel("Messages per topic")

plt.show()