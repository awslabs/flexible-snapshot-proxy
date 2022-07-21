"""
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
"""
import unittest
import sys
import os
import subprocess

sys.path.insert(1, f'{os.path.dirname(os.path.realpath(__file__))}/../src') #makes source code testable

from main import install_dependencies, dependency_checker, version_cmp

'''
Below are unit tests for the dependency checker in src/main.py
'''
class PackageVersionCMP(unittest.TestCase):
  def equal_length_less(self):
    v1 = "1.2.3"
    v2 = "1.2.4"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "10.2"
    v2 = "10.3"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "1"
    v2 = "2"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

  def equal_length_same(self):
    v1 = "1.2.3"
    v2 = "1.2.3"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "10.2"
    v2 = "10.2"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "1"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

  def equal_length_greater(self):
    v1 = "1.2.4"
    v2 = "1.2.3"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "10.3"
    v2 = "10.2"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "2"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

  def v1_shorter_less(self):
    v1 = "1.2"
    v2 = "1.2.4"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "10"
    v2 = "10.3"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "1.0"
    v2 = "2.1.3.26"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

  def v1_shorter_same(self):
    v1 = "1.2"
    v2 = "1.2.0"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "1"
    v2 = "1.0.0"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

  def v1_shorter_greater(self):
    v1 = "1.3"
    v2 = "1.2.3"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "10"
    v2 = "9.12.4"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "2"
    v2 = "1.7"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

  def v2_shorter_less(self):
    v1 = "0.2.26"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "10.2.98"
    v2 = "10.3"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

    v1 = "1.0.7.9"
    v2 = "2.3.5"
    self.assertTrue(version_cmp(v1,v2) < 0, f"version_cmp({v1},{v2}) should return < 0 not {version_cmp(v1,v2)}")

  def v2_shorter_same(self):
    v1 = "1.2.0"
    v2 = "1.2"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

    v1 = "1.0.0"
    v2 = "1"
    self.assertTrue(version_cmp(v1,v2) == 0, f"version_cmp({v1},{v2}) should return == 0 not {version_cmp(v1,v2)}")

  def v2_shorter_greater(self):
    v1 = "1.4.3"
    v2 = "1.3"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "11.12.4"
    v2 = "10"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

    v1 = "3.7"
    v2 = "2"
    self.assertTrue(version_cmp(v1,v2) > 0, f"version_cmp({v1},{v2}) should return > 0 not {version_cmp(v1,v2)}")

class DependencyCheckAndInstall(unittest.TestCase):
  requirements = ['aws-shell>=0.2.2', 'boto3>=1.24.22', 'botocore>=1.27.25',
  'joblib>=1.1.0', 'numpy>=1.21.6', 'ruamel.yaml>=0.17.21', 'ruamel.yaml.clib>=0.2.6',
  'urllib3>=1.26.9', 'zstandard>0,<0.18.0']
  curr_pip3_packages = []

  def setUp(self):
    super().setUp()

    self.package_names = ['aws-shell', 'boto3', 'botocore','joblib', 'numpy', 'ruamel.yaml', 'ruamel.yaml.clib', 'urllib3', 'zstandard']

    result = subprocess.run(["pip3", "freeze"], capture_output=True)
    self.curr_pip3_packages = result.stdout.decode('utf-8').split('\n')

  def fresh_install(self):
    mock_pip_freeze = ['']
    needs_install, needs_version_adjustment = dependency_checker(mock_pip_freeze, self.requirements)

    self.assertEqual(len(needs_install), len(self.requirements), "On Fresh Install. Does not recognize that all dependencies must be installed.")
    self.assertEqual(len(needs_version_adjustment), 0, "On Fresh Install, no packages need version adjustments.")

    counter = 0
    for p in self.package_names:
      self.assertTrue((p in needs_install), "Not all required dependencies found in install list.")
      counter += 1

    self.assertEqual(counter, 9, "On Fresh Install, Not all required packages would've been installed.")

  def all_installed(self):
    mock_pip_freeze = ['aws-shell==0.2.2', 'boto3==1.24.22', 'botocore==1.27.25',
    'joblib==1.1.0', 'numpy==1.21.6', 'ruamel.yaml==0.17.21', 'ruamel.yaml.clib==0.2.6',
    'urllib3==1.26.9', 'zstandard==0.18.0']
    needs_install, needs_version_adjustment = dependency_checker(mock_pip_freeze, self.requirements)

    self.assertEqual(len(needs_install), 0, "Did not recognize that all packages are already installed")
    self.assertEqual(len(needs_version_adjustment), 0, "Did not recognize that all packages are already installed")

  def mix_in_to_install_to_update_and_some_satisfied(self):
    '''
    Note:
    to install: numpy==1.21.6, 'boto3==1.24.22', 'botocore==1.27.25'
    to upgrade: 'aws-shell==0.2.2', 'zstandard==0.18.0'
    all good: 'joblib==1.1.0', 'ruamel.yaml==0.17.21', 'ruamel.yaml.clib==0.2.6', 'urllib3==1.26.9'
    '''
    mock_pip_freeze = ['aws-shell==0.2.1', 'joblib==1.1.0', 'ruamel.yaml==0.17.21',
    'ruamel.yaml.clib==0.2.6', 'urllib3==1.26.9', 'zstandard==0.19.0']

    needs_install, needs_version_adjustment = dependency_checker(mock_pip_freeze, self.requirements)

    self.assertEqual(len(needs_install), 3, "Did not identify all packages that need to be installed.")
    self.assertEqual(len(needs_version_adjustment), 2, "Did not identify all packages that need to be upgraded.")

    counter = 0
    for p in self.package_names:
      if p in needs_install:
        counter += 1
    self.assertEqual(counter, 3, "Did not identify all packages that need to be installed.")

    counter = 0
    for p in self.package_names:
      if p in needs_version_adjustment:
        counter += 1
    self.assertEqual(counter, 2, "Did not identify all packages that need to be upgraded.")


def DependencyCheckerSuite():
  suite = unittest.TestSuite()

  suite.addTest(PackageVersionCMP('equal_length_less'))
  suite.addTest(PackageVersionCMP('equal_length_same'))
  suite.addTest(PackageVersionCMP('equal_length_greater'))
  suite.addTest(PackageVersionCMP('v1_shorter_less'))
  suite.addTest(PackageVersionCMP('v1_shorter_same'))
  suite.addTest(PackageVersionCMP('v1_shorter_greater'))
  suite.addTest(PackageVersionCMP('v2_shorter_less'))
  suite.addTest(PackageVersionCMP('v2_shorter_same'))
  suite.addTest(PackageVersionCMP('v2_shorter_greater'))

  suite.addTest(DependencyCheckAndInstall('fresh_install'))
  suite.addTest(DependencyCheckAndInstall('all_installed'))
  suite.addTest(DependencyCheckAndInstall('mix_in_to_install_to_update_and_some_satisfied'))

  return suite