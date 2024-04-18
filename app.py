import sys, json, psycopg2
from pathlib import Path
from PyQt5.QtWidgets import QMainWindow, QApplication, QMessageBox, QTableWidget, QTableWidgetItem
from PyQt5 import uic
from typing import Any


qtCreatorFile = Path("milestone3.ui")

Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)

class myApp(QMainWindow):
    def __init__(self, config_file: Path):
        super(myApp, self).__init__()
        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        config = get_pg_config_str(config_file)
        try:
            self.conn = psycopg2.connect(config)
        except psycopg2.errors.OperationalError:
            print("Unable to connect to the database!")
            self.error_box("Unable to connect to the database!", "Connection Error")
            exit()

        self.cur = self.conn.cursor()

        self.ui.state_combo.currentTextChanged.connect(self.state_changed)
        self.ui.city_list.currentItemChanged.connect(self.city_changed)
        self.ui.zip_list.currentItemChanged.connect(self.zip_changed)
        self.ui.category_list.currentItemChanged.connect(self.category_changed)
        self.ui.refresh_button.clicked.connect(self.updateSuccessfulPopular)
        self.ui.zipstatistics_categories.horizontalHeader().setStretchLastSection(True)

        self.init_states()

        # column names in Business table and display name in UI
        self.business_columns = [
            ("b.name", "Name"),
            ("b.address", "Address"),
            ("b.city", "City"),
            ("b.stars", "Stars"),
            ("b.review_rating", "Review Rating"),
            ("b.review_count", "Review Count"),
            ("b.num_checkins", "Checkin Count")
        ]

    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except AttributeError:
            pass

    def error_box(self, message: str, title: str):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setText(message)
        msg.setWindowTitle(title)
        msg.setStandardButtons(QMessageBox.Ok)
        msg.exec_()

    def init_states(self):
        query = """
            SELECT DISTINCT state
            FROM Business
            ORDER BY state;
        """
        state_tuples = select_query(self.cur, query)
        state_strings = extract_singletons(state_tuples)

        self.ui.state_combo.addItems(state_strings)

    def state_changed(self):
        self.ui.city_list.clear()
        selected_state = self.ui.state_combo.currentText()
        query = f"""
            SELECT DISTINCT city
            FROM Business
            WHERE state='{selected_state}'
            ORDER BY city;
        """
        city_tuples = select_query(self.cur, query)
        city_strings = extract_singletons(city_tuples)

        self.ui.city_list.addItems(city_strings)

    def city_changed(self):
        self.ui.zip_list.clear()

        try:
            selected_state = self.ui.state_combo.currentText()
            self.ui.refresh_button.setEnabled(False)
            selected_city = self.ui.city_list.currentItem().text()
        except AttributeError:
            return

        query = f"""
            SELECT DISTINCT zipcode
            FROM Business
            WHERE city='{selected_city}' AND state='{selected_state}'
            ORDER BY zipcode;
        """
        zip_tuples = select_query(self.cur, query)
        zip_strings = extract_singletons(zip_tuples)

        self.ui.zip_list.addItems(zip_strings)

    def zip_changed(self):
        # clear all tables dependent on zipcode
        self.ui.category_list.clear()
        self.ui.zipstatistics_categories.clear()
        self.ui.zipstatistics_businesses.clear()
        self.ui.zipstatistics_population.clear()
        self.ui.zipstatistics_income.clear()

        clearTable(self.ui.successful_table)
        clearTable(self.ui.popular_table)
        clearTable(self.ui.zipstatistics_categories)
        clearTable(self.ui.business_table)

        try:
            selected_zip = self.ui.zip_list.currentItem().text().strip()
            self.ui.refresh_button.setEnabled(True)  # only set enabled if a zip code is selected, not on table clear
            self.updateZipStatistics(selected_zip)
        except AttributeError:
            return

        query = f"""
            SELECT DISTINCT c.cat_name
            FROM Business b
            JOIN Categories c ON b.business_id = c.business_id
            WHERE b.zipcode='{selected_zip}'
            ORDER BY c.cat_name;
        """
        category_tuples = select_query(self.cur, query)
        category_strings = extract_singletons(category_tuples)

        self.ui.category_list.addItems(category_strings)

        query = f"""
            SELECT {', '.join([c[0] for c in self.business_columns])}
            FROM Business b
            WHERE b.zipcode='{selected_zip.strip()}'
            ORDER BY name;
        """
        business_tuples = select_query(self.cur, query)

        updateTable(self.ui.business_table, business_tuples, [h[1] for h in self.business_columns])

    def updateZipStatistics(self, zipcode: str):
        zipquery = f"""
            SELECT meanIncome, population
            FROM zipcodeData
            WHERE zipcode='{zipcode}';
        """

        mean_income, population = select_query(self.cur, zipquery)[0]  # will always return 1 tuple
        self.ui.zipstatistics_income.setText(str(mean_income))
        self.ui.zipstatistics_population.setText(str(population))

        businessquery = f"""
            SELECT COUNT(*)
            FROM Business
            WHERE zipcode='{zipcode}';
        """

        num_businesses = select_query(self.cur, businessquery)[0][0]
        self.ui.zipstatistics_businesses.setText(str(num_businesses))

        catquery = f"""
            SELECT COUNT(*) as count, c.cat_name
            FROM Business b
            JOIN Categories c ON b.business_id = c.business_id
            WHERE zipcode='{zipcode}'
            GROUP BY c.cat_name
            ORDER BY count DESC;
        """

        business_tuples = select_query(self.cur, catquery)

        updateTable(self.ui.zipstatistics_categories, business_tuples, ["Count", "Category"])

    def category_changed(self):
        clearTable(self.ui.business_table)

        try:
            selected_zip = self.ui.zip_list.currentItem().text()
            selected_category = self.ui.category_list.currentItem().text()
        except AttributeError:
            return

        query = f"""
            SELECT {', '.join([c[0] for c in self.business_columns])}
            FROM Business b
            JOIN Categories c ON b.business_id = c.business_id
            WHERE b.zipcode='{selected_zip.strip()}' AND c.cat_name='{selected_category}'
            ORDER BY name;
        """
        business_tuples = select_query(self.cur, query)

        updateTable(self.ui.business_table, business_tuples, [h[1] for h in self.business_columns])

    def updateSuccessfulPopular(self):
        queries_tables_columns = [
            (Path("queries/successful.sql"), self.ui.successful_table, ["Name", "Category", "Stars", "Review Count", "Checkin Count"]),
            (Path("queries/popular.sql"), self.ui.popular_table, ["Name", "Stars", "Review Rating", "Review Count"])
        ]

        for tup in queries_tables_columns:
            path, table, columns = tup
            with open(path, 'r') as f:
                selected_zip = self.ui.zip_list.currentItem().text()
                query = f.read().replace("{zipcode}", selected_zip.strip())
                business_tuples = select_query(self.cur, query)

                updateTable(table, business_tuples, columns)

def get_pg_config_str(config_path: Path) -> str:
    with open(config_path, "r") as file:
        data = json.load(file)

    return f"dbname='{data['dbname']}' user='{data['user']}' host='{data['host']}' password='{data['password']}'"

def select_query(cur: psycopg2.extensions.cursor, query: str) -> list[tuple[Any,...]]:
    cur.execute(query)
    return cur.fetchall()

def extract_singletons(tuples: list[tuple[Any,]]) -> list[Any]:
    extract = lambda x: x[0]
    return list(map(extract, tuples))

def clearTable(table: QTableWidget) -> None:
    table.clearContents()
    table.setRowCount(0)
    table.setColumnCount(0)

def updateTable(table: QTableWidget, business_tuples: tuple[Any,...], headers: list[str]) -> None:
    business_ct = len(business_tuples)
    attr_ct = len(headers)

    table.setRowCount(business_ct)
    table.setColumnCount(attr_ct)
    table.setHorizontalHeaderLabels(headers)

    for business in range(business_ct):  # preserve indices to set cell location
        for attr in range(attr_ct):
            data = QTableWidgetItem(f"{business_tuples[business][attr]}")
            table.setItem(business, attr, data)

if __name__ == "__main__":
    config_file = Path("pg_config.json")

    app = QApplication(sys.argv)
    window = myApp(config_file)
    window.show()
    sys.exit(app.exec_())
