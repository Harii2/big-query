from typing import List


class TableNamesMappingNotFound(Exception):
    def __init__(self, table_names: List[str]):
        self.table_names = table_names


class NoMappingFoundForFieldNames(Exception):
    def __init__(self, field_names: List[str]):
        self.field_names = field_names
