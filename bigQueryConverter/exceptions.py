class TableNameMappingNotFound(Exception):
    def __init__(self, table_name: str):
        self.table_name = table_name
