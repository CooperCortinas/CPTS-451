import sys, json, psycopg2
from pathlib import Path
from PyQt5.QtWidgets import QMainWindow, QApplication, QWidget, QAction, QTableWidget,QTableWidgetItem,QVBoxLayout
from PyQt5 import uic, QtCore
from PyQt5.QtGui import QIcon, QPixmap


# qtCreatorFile = "MyUI.ui" # Enter file here.

# Ui_MainWindow, QtBaseClass = uic.loadUiType(qtCreatorFile)

# class myApp(QMainWindow):
#     def __init__(self):
#         super(myApp, self).__init__()
#         self.ui = Ui_MainWindow()
#         self.ui.setupUi(self)

def get_pg_config_str(config_path: Path) -> str:
    with open(config_path, "r") as file:
        data = json.load(file)

    return f"dbname='{data['dbname']}' user='{data['user']}' host='{data['host']}' password='{data['password']}'"

def select_query(cur: psycopg2.extensions.cursor, query: str) -> list[tuple]:
    cur.execute(query)
    return cur.fetchall()

if __name__ == "__main__":
    config_file = Path("pg_config.json")
    config = get_pg_config_str(config_file)

    try:
        conn = psycopg2.connect(config)
    except psycopg2.errors.OperationalError:
        print("Unable to connect to the database!")
        exit()

    with conn.cursor() as cur:
        query = """
            SELECT DISTINCT state
            FROM business
            ORDER BY state;
        """
        states = select_query(cur, query)
        selected_state = states[0][0]

        query = f"""
            SELECT DISTINCT city
            FROM business
            WHERE state='{selected_state}'
            ORDER BY city;
        """
        cities = select_query(cur, query)
        selected_city = cities[0][0]

        query = f"""
            SELECT name, city, state
            FROM business
            WHERE city='{selected_city}' AND state='{selected_state}'
            ORDER BY name;
        """
        businesses = select_query(cur, query)
        selected_business = businesses[0][0]
        print(selected_business)

    conn.close()

    # app = QApplication(sys.argv)
    # window = myApp()
    # window.show()
    # sys.exit(app.exec_())
