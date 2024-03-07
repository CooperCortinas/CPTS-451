import sys, json, psycopg2
from pathlib import Path
from PyQt5.QtWidgets import QMainWindow, QApplication, QMessageBox
from PyQt5 import uic
from typing import Any


qtCreatorFile = Path("milestone1.ui")

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

        self.init_states()

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
            FROM business
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
            FROM business
            WHERE state='{selected_state}'
            ORDER BY city;
        """
        city_tuples = select_query(self.cur, query)
        city_strings = extract_singletons(city_tuples)

        self.ui.city_list.addItems(city_strings)

    def city_changed(self):
        self.ui.business_list.clear()
        selected_state = self.ui.state_combo.currentText()
        try:
            selected_city = self.ui.city_list.currentItem().text()
        except AttributeError:
            return

        query = f"""
            SELECT name, city, state
            FROM business
            WHERE city='{selected_city}' AND state='{selected_state}'
            ORDER BY name;
        """
        business_tuples = select_query(self.cur, query)
        business_strings = extract_singletons(business_tuples)

        self.ui.business_list.addItems(business_strings)

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

if __name__ == "__main__":
    config_file = Path("pg_config.json")

    app = QApplication(sys.argv)
    window = myApp(config_file)
    window.show()
    sys.exit(app.exec_())
