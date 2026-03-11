import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import joblib

DATA_PATH = "data/tickets_with_embeddings.pkl"

MODEL_PATH_REQUEST = "models/request_type_model.pkl"
MODEL_PATH_ISSUE = "models/issue_type_model.pkl"

df = pd.read_pickle(DATA_PATH)

X = np.vstack(df["embedding"].values)

y_request = df["request_type"]

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y_request,
    test_size=0.2,
    random_state=42,
    stratify=y_request
)

clf_request = LogisticRegression(max_iter=2000)
clf_request.fit(X_train, y_train)

pred = clf_request.predict(X_test)

print("\nREQUEST TYPE REPORT\n")
print(classification_report(y_test, pred))

joblib.dump(clf_request, MODEL_PATH_REQUEST)

print("Request type model saved")


df_issue = df[df["request_type"] == "analysis_request"]

X_issue = np.vstack(df_issue["embedding"].values)
y_issue = df_issue["issue_type"]

X_train, X_test, y_train, y_test = train_test_split(
    X_issue,
    y_issue,
    test_size=0.2,
    random_state=42,
    stratify=y_issue
)

clf_issue = LogisticRegression(max_iter=2000)
clf_issue.fit(X_train, y_train)

pred = clf_issue.predict(X_test)

print("\nISSUE TYPE REPORT\n")
print(classification_report(y_test, pred))

joblib.dump(clf_issue, MODEL_PATH_ISSUE)

print("Issue type model saved")