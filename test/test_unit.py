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

sys.path.insert(1, '../src') #makes source code testable

from main import install_dependencies

'''
Below are unit tests for the dependency checker in src/main.py
'''
class AllDependenciesInstalled(unittest.TestCase):
  def setUp(self):
    super().setUp()

    subprocess.run(['pip3', 'install', '-q', '-r', f'{os.path.dirname(os.path.realpath(__file__))}/../requirements.txt'])

  def all_clear(self):
    self.assertTrue(install_dependencies(), "Dependencies were installed, but installer failed.")

class DependenciesOutOfDate(unittest.TestCase):
  def one_out_of_date(self):
    #downgrade aws cli 
    subprocess.run(['pip3', 'uninstall', '--yes', 'aws-shell', '-q'])
    subprocess.run(['pip3', 'install', 'aws-shell==0.1.1', '-q'])
    self.assertTrue(install_dependencies(), "Should have upgraded aws cli and returned True")

class DependenciesNotPresent(unittest.TestCase):
  def missing_boto3(self):
    #downgrade aws cli 
    subprocess.run(['pip3', 'uninstall', '--yes', 'boto3', '-q'])
    self.assertTrue(install_dependencies(), "Should have installed boto3 and returned True")

def DependencyCheckerSuite():
  suite = unittest.TestSuite()

  suite.addTest(AllDependenciesInstalled('all_clear'))
  suite.addTest(DependenciesOutOfDate('one_out_of_date'))
  suite.addTest(DependenciesNotPresent('missing_boto3'))

  return suite