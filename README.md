# CPTS-451 - Business Analytics
Project for CPTS_451 using the Yelp and US Census datasets, enabling users to explore businesses by location, category, popularity, and success metrics.

With Python 3 installed, all required packages can be install using:
```bash
pip install -r requirements.txt
```
Extract [yelp_dataset.7z](./yelp_dataset.7z) to the project root directory. Set your database credentials in [pg_config.json](./pg_config.json), then insert the data with:
```bash
python3 inserting_data.py
```
Run the application:
```bash
python3 app.py
```
