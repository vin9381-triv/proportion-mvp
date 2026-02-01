from transformers import pipeline

classifier = pipeline(
    "zero-shot-classification",
    model="M-FAC/bert-mini-finetuned-mnli"
)

result = classifier(
    "I like you. I love you",
    candidate_labels=[
        "supports the policy",
        "opposes the policy",
        "is neutral toward the policy"
    ]
)

print(result)
