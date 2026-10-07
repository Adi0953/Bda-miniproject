# RailBigData

Railway complaint analytics dashboard with a transparent priority estimate and a starter Apache Spark batch job.

## Run the web app

Run `start_server.bat` on Windows, then open `http://localhost:8000`. Node.js is required; there are no npm packages to install. Keep the terminal window open while using the app. You can also open `index.html` directly in a modern browser. The dashboard starts with a built-in sample, and you can load another CSV with **Upload dataset**. Filtering, charts, priority estimates, and CSV export run locally in the browser; the uploaded file is not sent anywhere.

## Dataset to use

Use historical railway passenger grievance/complaint records in CSV format. For a class project, aim for at least 1,000 records; multiple months and zones enable trend analysis. Public railway complaint data is often published as dashboards rather than downloadable row-level files, so check whether your railway or transport authority provides an open data CSV. Otherwise, request an anonymized export or use a clearly labelled synthetic dataset for a prototype. Do not publish passenger names, phone numbers, booking references, or other personal information.

### Your `train.csv`

This file has 1,366 rows and the columns `Item ID`, `Sentiment`, and `SentimentText`. The dashboard now accepts it directly: it uses the ID and text, infers a broad complaint category from the text, and displays `Sentiment` as an original source label. The file does not include dates, zones, statuses, resolution times, or definitions for its numeric sentiment labels, so those analyses cannot be derived reliably from this file. Priority estimates are separate rule-based estimates and must not be confused with the sentiment labels. The browser app redacts @handles and long digit strings in the text before displaying or exporting it. Upload it with **Upload dataset**; the original file is not modified.

`rail_complaints_app.csv` is a sanitized copy prepared from the provided file. Its complaint text has @handles, long digit strings, and email addresses redacted. Use this copy when importing the sample into the dashboard, and review it before sharing publicly.

Required columns:

| Column | Meaning | Example |
| --- | --- | --- |
| `date` | Complaint date, preferably `YYYY-MM-DD` | `2026-09-01` |
| `complaint_type` | Issue category | `Delay` |

Recommended columns:

| Column | Meaning | Example |
| --- | --- | --- |
| `complaint_id` | Anonymized unique ID | `RB-1001` |
| `description` | Short complaint text with personal details removed | `Train delayed by 75 minutes` |
| `zone` | Railway zone, division, or region | `Central` |
| `status` | Current/final state | `Resolved` |
| `resolution_hours` | Hours until closure; blank for unresolved cases | `8` |
| `impact_score` | Optional impact severity, 1 (low) to 5 (highest) | `4` |
| `priority` | Historical human-assigned priority label | `High` |

`priority` is the target label needed to train and evaluate a supervised prediction model. The dashboard displays a rule-based estimate when loading data; it is not a trained model. The optional `priority` column lets you compare the estimate with historical labels. The included 30-row `sample_complaints.csv` is synthetic demonstration data, not an official railway dataset.

## Spark batch analytics

The starter PySpark job computes scalable complaint counts by month, category, zone, and estimated priority. Install Java and PySpark, then run:

```powershell
python -m pip install pyspark
spark-submit spark/aggregate_complaints.py sample_complaints.csv output
```

The same job accepts your `train.csv` directly and writes its grouped results under `output`:

```powershell
spark-submit spark/aggregate_complaints.py C:\Users\ritha_000\Downloads\train.csv output
```

For supervised prediction, use records with reliable human-labelled `priority` values. Split data into train/test sets, encode category and text features, train a Spark ML classifier, and report a confusion matrix, macro F1, and per-class recall. Treat safety-related high priority recall as especially important. The web dashboard's current rule estimate should not be represented as that trained classifier.
