import os
import pandas as pd
import numpy as np
import logging
import torch
from sentence_transformers import SentenceTransformer
import umap
import hdbscan
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score, davies_bouldin_score
import plotly.express as px
from joblib import Parallel, delayed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# Configuration
# =========================
DATA_FILE = 'youtube_comments_full.csv'
SAMPLE_SIZE = 20000     # number of comments to sample for pipeline
EMBED_MODEL = 'jhgan/ko-sbert-sts'
UMAP_NEIGHBORS = 10
UMAP_COMPONENTS = 5
HDBSCAN_MIN_CLUSTER_SIZE = 100
HDBSCAN_EPSILON = 0.1
KMEANS_K_LIST = [5,10,15]
GMM_K_LIST = [5,10,15]
TOPIC_CANDIDATES = ['이재명','안철수','한동훈','윤석열']
OUTPUT_HTML = 'umap_clusters.html'
OUTPUT_SUMMARY = 'cluster_topic_summary.csv'

# =========================
# 1. Load and sample data
# =========================
df = pd.read_csv(DATA_FILE, encoding='utf-8-sig')
if len(df) > SAMPLE_SIZE:
    df = df.sample(n=SAMPLE_SIZE, random_state=42).reset_index(drop=True)
logger.info(f"Loaded {len(df)} comments for processing")

# =========================
# 2. Embedding (CPU only)
# =========================
torch.set_num_threads(4)
device = torch.device('cpu')
model = SentenceTransformer(EMBED_MODEL, device=device)
texts = df['comment_text'].fillna('').tolist()
embeddings = model.encode(texts, batch_size=32, show_progress_bar=True)
embeddings = np.array(embeddings, dtype=np.float32)

# =========================
# 3. UMAP reduction
# =========================
logger.info(f"Reducing to {UMAP_COMPONENTS}D with UMAP (n_neighbors={UMAP_NEIGHBORS})")
umap_reducer = umap.UMAP(n_neighbors=UMAP_NEIGHBORS, n_components=UMAP_COMPONENTS,
                        metric='cosine', random_state=42)
embed_5d = umap_reducer.fit_transform(embeddings)
logger.info(f"UMAP 5D shape: {embed_5d.shape}")

# =========================
# 4. HDBSCAN clustering
# =========================
logger.info(f"Clustering with HDBSCAN (min_cluster_size={HDBSCAN_MIN_CLUSTER_SIZE}, epsilon={HDBSCAN_EPSILON})...")
hdb = hdbscan.HDBSCAN(min_cluster_size=HDBSCAN_MIN_CLUSTER_SIZE,
                      cluster_selection_epsilon=HDBSCAN_EPSILON,
                      metric='euclidean', core_dist_n_jobs=-1)
labels_hdb = hdb.fit_predict(embed_5d)
n_hdb = len(set(labels_hdb)) - (1 if -1 in labels_hdb else 0)
sil_hdb = silhouette_score(embed_5d[labels_hdb>=0], labels_hdb[labels_hdb>=0])
dbi_hdb = davies_bouldin_score(embed_5d[labels_hdb>=0], labels_hdb[labels_hdb>=0])
logger.info(f"HDBSCAN clusters (excluding noise): {n_hdb}")
logger.info(f"HDBSCAN Silhouette: {sil_hdb:.4f}, DBI: {dbi_hdb:.4f}")

# =========================
# 5. KMeans and GMM comparison
# =========================
for k in KMEANS_K_LIST:
    km = KMeans(n_clusters=k, random_state=42)
    km_labels = km.fit_predict(embed_5d)
    sil = silhouette_score(embed_5d, km_labels)
    dbi = davies_bouldin_score(embed_5d, km_labels)
    logger.info(f"KMeans k={k} Silhouette: {sil:.4f}, DBI: {dbi:.4f}")

for k in GMM_K_LIST:
    gm = GaussianMixture(n_components=k, random_state=42)
    gm_labels = gm.fit_predict(embed_5d)
    sil = silhouette_score(embed_5d, gm_labels)
    dbi = davies_bouldin_score(embed_5d, gm_labels)
    logger.info(f"GMM k={k} Silhouette: {sil:.4f}, DBI: {dbi:.4f}")

# =========================
# 6. 2D UMAP for visualization
# =========================
embed_2d = umap.UMAP(n_neighbors=UMAP_NEIGHBORS, n_components=2,
                      metric='cosine', random_state=42).fit_transform(embeddings)
fig = px.scatter(x=embed_2d[:,0], y=embed_2d[:,1], color=labels_hdb.astype(str),
                 title="HDBSCAN Clusters", labels={'color':'cluster'})
fig.write_html(OUTPUT_HTML)
logger.info(f"Plot saved to {OUTPUT_HTML}")

# =========================
# 7. Topic assignment and merging
# =========================
# Assign each HDBSCAN cluster a candidate topic by counting occurrences
cluster_topics = {}
for cid in set(labels_hdb):
    if cid < 0: continue
    texts_c = df[np.array(labels_hdb)==cid]['comment_text'].tolist()
    counts = {t: sum(t in txt for txt in texts_c) for t in TOPIC_CANDIDATES}
    # pick topic with max count, default 'Other'
    top = max(counts, key=lambda t: counts[t]) if max(counts.values())>0 else 'Other'
    cluster_topics[cid] = top
# Merge clusters: relabel all points by topic
merged_labels = np.array([cluster_topics.get(lbl,'Other') if lbl>=0 else 'Noise' for lbl in labels_hdb])

# Save topic summary
summary = pd.DataFrame([{'cluster':cid,'size':sum(labels_hdb==cid),'topic':cluster_topics.get(cid,'Other')}
                        for cid in sorted(cluster_topics)])
summary.to_csv(OUTPUT_SUMMARY, index=False, encoding='utf-8-sig')
logger.info(f"Cluster-topic summary saved to {OUTPUT_SUMMARY}")

# End of pipeline
