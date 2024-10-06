from Orange.data import Table
from Orange.widgets import gui
from Orange.widgets.settings import Setting
from Orange.widgets.widget import OWWidget, Output

import sqlalchemy as db
from Orange.data.pandas_compat import table_from_frame
import Orange.data.pandas_compat as pc

from AnyQt.QtWidgets import QLabel, QLineEdit, QTextEdit




class DatabricksWidget(OWWidget):
    name = "Databricks"
    description = "Load dataset from Databricks."
    icon = "icons/databricks.svg"
    priority = 100
    keywords = ["widget", "data"]
    want_main_area = False
    resizing_enabled = True
    
    server_hostname = Setting("")
    http_path = Setting("")
    access_token = Setting("")
    catalog = Setting("")
    schema = Setting("")
    query = Setting("")
        
        

        
    class Outputs:
        data = Output("Data", Table, default=True)




    def __init__(self):
        super().__init__()
        
        self.server_hostname_box = gui.lineEdit(
            self.controlArea, self, "server_hostname", label="Host")
        
        self.http_path_box = gui.lineEdit(
            self.controlArea, self, "http_path", label="HTTP Path")
        
        self.access_token_box = gui.lineEdit(
            self.controlArea, self, "access_token", label="Token")
        self.access_token_box.setEchoMode(QLineEdit.Password)
        
        self.catalog_box = gui.lineEdit(
            self.controlArea, self, "catalog", label="Catalog")
        
        self.schema_box = gui.lineEdit(
            self.controlArea, self, "schema", label="Schema")
        
        self.controlArea.layout().insertWidget(5, QLabel("Query"))
        self.query_box = QTextEdit(self.query)
        self.controlArea.layout().insertWidget(6, self.query_box)
        
        self.button = gui.button(
            self.controlArea, self, "Execute", callback=self.button_onclick)
        
    
    
    
    def button_onclick(self):
        self.save_query()
        
        self.error()
        self.pb = gui.ProgressBar(self, 100)
        self.pb.advance(33)
        self.repaint()
        
        try: 
            self.execute_query()
        except BaseException as e:
            self.error(str(e))
        finally:
            self.pb.finish()
    
    
    
    
    def save_query(self):
        self.query = self.query_box.toPlainText()
    
    
    
    
    def execute_query(self):        
        server_hostname = self.server_hostname_box.text()
        http_path       = self.http_path_box.text()
        access_token    = self.access_token_box.text()
        catalog         = self.catalog_box.text()
        schema          = self.schema_box.text()
        query           = self.query_box.toPlainText()

        engine =  db.create_engine(
          url = f"databricks://token:{access_token}@{server_hostname}?" +
                f"http_path={http_path}&catalog={catalog}&schema={schema}"
        )

        con = engine.connect()
        result = pc.pd.read_sql(query, con)
        engine.dispose()
        self.Outputs.data.send(table_from_frame(result))
        
     
        

if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview
    WidgetPreview(DatabricksWidget).run()
