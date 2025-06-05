import base64
from io import BytesIO
import pandas as pd
from sklearn.metrics import confusion_matrix, classification_report
import seaborn as sns
from flask import Flask
from matplotlib.figure import Figure
import psycopg2

app = Flask(__name__)


@app.route("/")
def hello():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="db",
    )
    cursor = conn.cursor()

    # Execute query to get data
    cursor.execute("SELECT * FROM transactions;")
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    df = pd.DataFrame(rows, columns=colnames)

    df["predicted_label"] = df["category"].apply(
        lambda category: 1 if category == "FRAUD" else 0
    )
    df["gt_label"] = df["gt_is_fraud"].astype(int)

    cm = confusion_matrix(df["gt_label"], df["predicted_label"])

    fig = Figure()
    ax = fig.subplots()
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=["Pred: 0", "Pred: 1"],
        yticklabels=["True: 0", "True: 1"],
        ax=ax,
    )

    buf = BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)
    data = base64.b64encode(buf.getvalue()).decode("utf-8")

    report = classification_report(
        df["gt_label"], df["predicted_label"], output_dict=True
    )
    report_html = classification_report_dict_to_html(report)

    return f"<h1>Confusion Matrix</h1><img src='data:image/png;base64,{data}'/><div>{report_html}</div>"


def classification_report_dict_to_html(report: dict) -> str:
    all_columns = set()
    for key, value in report.items():
        if isinstance(value, dict):
            all_columns.update(value.keys())
    all_columns = list(all_columns)

    html = "<h2>Classification Report</h2><table border='1' cellpadding='4' cellspacing='0' style='border-collapse: collapse;'>"

    html += (
        "<tr><th>Label</th>"
        + "".join(f"<th>{col}</th>" for col in all_columns)
        + "</tr>"
    )

    for label, metrics in report.items():
        if isinstance(metrics, dict):
            html += (
                f"<tr><td>{label}</td>"
                + "".join(
                    (
                        f"<td>{metrics.get(col, ''):.2f}"
                        if isinstance(metrics.get(col, ""), (float, int))
                        else f"<td>{metrics.get(col, '')}</td>"
                    )
                    for col in all_columns
                )
                + "</tr>"
            )
        elif label == "accuracy":
            html += f"<tr><td>{label}</td><td colspan='{len(all_columns)}'>{metrics:.2f}</td></tr>"

    html += "</table>"
    return html


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
