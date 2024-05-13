from sql_server_lineage import get_lineage, generate_html_lineage
from unittest import TestCase, main
from typing import Dict
import os

class TestGetLineage(TestCase):
    def setUp(self):
        self.test_file: str = 'test_data/table_variable_extraction.txt'
        self.expected_lineage: Dict = {
            "prefix1_shows.schema.table_test": {
                "schema.table_variable_extraction": [
                    "db.schema.something",
                    "other_source",
                    "prefix1_prefix2.schema.test2",
                    "prefix1_shows.schema.table_test3",
                ],
            },
            "prefix1_shows.schema.table_test6": {
                "schema.table_variable_extraction": [
                    "prefix1_shows.schema.table_test3",
                ],
            },
        }

    def test_get_lineage(self):
        with open(self.test_file, 'r', encoding="utf-8") as f:
            sp: str = f.read()
        result_lineage: Dict = get_lineage([sp])
        result_lineage["prefix1_shows.schema.table_test"]["schema.table_variable_extraction"] = sorted(
            result_lineage["prefix1_shows.schema.table_test"]["schema.table_variable_extraction"]
        )
        self.assertDictEqual(
            result_lineage,
            self.expected_lineage,
            "The lineage produced is not the expected one.",
        )

    def test_generate_html_lineage(self):
        with open(self.test_file, 'r', encoding="utf-8") as f:
            sp: str = f.read()
        generate_html_lineage([sp])
        html_files = [f for f in os.listdir() if os.path.isfile(f) and '.html' in f]
        print(html_files)
        self.assertEqual(len(html_files), 1, "No html files produced.")


if __name__ == "__main__":
    main()