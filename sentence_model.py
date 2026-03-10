from sentence_transformers import SentenceTransformer

print("Chargement du modèle...")

model = SentenceTransformer(
    "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
)

print("Sauvegarde du modèle...")

model.save("multilingual_minilm_model")

print("Terminé.")