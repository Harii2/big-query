from typing import List


class TableNameMappingNotFound(Exception):
    def __init__(self, table_name: str):
        self.table_name = table_name


class NoMappingFoundForFieldNames(Exception):
    def __init__(self, field_names: List[str]):
        self.field_names = field_names
