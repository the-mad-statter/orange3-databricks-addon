import concurrent.futures
import logging
import Orange.data.pandas_compat as pc
import random
import sqlalchemy as db

from AnyQt.QtCore import QThread, pyqtSlot
from AnyQt.QtWidgets import QLabel, QLineEdit, QTextEdit
from functools import partial
from Orange.data import Table
from Orange.widgets import gui
from Orange.widgets.settings import Setting
from Orange.widgets.widget import OWWidget, Output
from orangewidget.utils.concurrent import FutureWatcher, methodinvoke




class Task:
    future = ...
    watcher = ...
    cancelled = False

    def cancel(self):
        self.cancelled = True
        self.future.cancel()
        concurrent.futures.wait([self.future])




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
        
        self._task = None
        self._executor = concurrent.futures.ThreadPoolExecutor()
        
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
        
        if self._task is not None:
            self.cancel()
        assert self._task is None
        
        self._task = task = Task()
        
        set_progress = methodinvoke(self, "setProgressValue", (float,))
        
        def callback(finished):
            if task.cancelled:
                raise KeyboardInterrupt()
            set_progress(finished * 100)
            
        execute_query = partial(self.execute_query, callback=callback)
        
        self.error()
        self.progressBarInit()
        task.future = self._executor.submit(execute_query)
        task.watcher = FutureWatcher(task.future)
        task.watcher.done.connect(self._task_finished)
        
        
        
        
    @pyqtSlot(float)
    def setProgressValue(self, value):
        assert self.thread() is QThread.currentThread()
        self.progressBarSet(value)
        
        
        
        
    @pyqtSlot(concurrent.futures.Future)
    def _task_finished(self, f):
        assert self.thread() is QThread.currentThread()
        assert self._task is not None
        assert self._task.future is f
        assert f.done()

        self._task = None
        self.progressBarFinished()

        try:
            data = pc.table_from_frame(f.result())
            self.Outputs.data.send(data)
        except Exception as ex:
            log = logging.getLogger()
            log.exception(__name__, exc_info=True)
            self.error("Exception occurred during evaluation: {!r}"
                       .format(ex))
            
            
            
            
    def cancel(self):
        if self._task is not None:
            self._task.cancel()
            assert self._task.future.done()
            self._task.watcher.done.disconnect(self._task_finished)
            self._task = None
            self.progressBarFinished()




    def onDeleteWidget(self):
        self.cancel()
        super().onDeleteWidget()
            
            
            
    
    def save_query(self):
        self.query = self.query_box.toPlainText()
    
    
    
    
    def execute_query(self, callback=None):
        if callback is not None:
            callback(random.uniform(0, 1))
        
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

        return result
        
     
        

if __name__ == "__main__":
    from Orange.widgets.utils.widgetpreview import WidgetPreview
    WidgetPreview(DatabricksWidget).run()
