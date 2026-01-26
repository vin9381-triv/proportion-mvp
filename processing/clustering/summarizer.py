from collections import defaultdict
from datetime import datetime

def summarize_clusters_to_file(labels, articles, output_path):
    """
    Writes human-readable cluster summaries to a text file.
    """
    clusters = defaultdict(list)

    for label, article in zip(labels, articles):
        clusters[label].append(article)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Proportion — Story Clustering Output\n")
        f.write(f"Generated at: {datetime.utcnow().isoformat()} UTC\n")
        f.write("=" * 80 + "\n\n")

        for label, members in sorted(
            clusters.items(),
            key=lambda x: len(x[1]),
            reverse=True
        ):
            f.write("=" * 80 + "\n")
            f.write(f"Cluster ID: {label}\n")
            f.write(f"Cluster Size: {len(members)}\n\n")

            for article in members:
                title = article.get("title", "").strip()

                published_raw = article.get("published_at_raw", "N/A")
                published_utc = article.get("published_at_utc", "N/A")

                f.write(f"- {title}\n")
                f.write(
                    f"  Published (raw): {published_raw} | "
                    f"Published (utc): {published_utc}\n"
                )

            f.write("\n")
