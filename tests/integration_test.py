import unittest
import subprocess
import os

class IntegrationTests(unittest.TestCase):
    input_file_paths = [f for f in os.listdir('.') if os.path.isfile(f) if f.startswith("test_input_") and f.endswith(".sql")]

    def test_transform_valid_input(self):
        for input_file_path in self.input_file_paths:
          test_case_id = input_file_path[11:][:-4]
          actual_output_file_path = "test_output_{}.sql".format(test_case_id)
          expected_output_file_path = "test_expected_{}.sql".format(test_case_id)
          subprocess.run(["python3", "../check_constraint.py", "transform", input_file_path, actual_output_file_path])
          with open(actual_output_file_path) as actual_output_file:
             actual_output_file_text = actual_output_file.readlines()
          with open(expected_output_file_path) as expected_output_file:
             expected_output_file_text = expected_output_file.readlines()
          self.assertEqual(actual_output_file_text, expected_output_file_text)

unittest.main()