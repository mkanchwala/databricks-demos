# evaluatenotebookruns.py
#!/usr/bin/python3
import io
import xmlrunner
from xmlrunner.extra.xunit_plugin import transform
import unittest
import json
import glob
import os

class TestJobOutput(unittest.TestCase):

  test_output_path = '<path-to-json-logs-on-release-agent>'

  def test_performance(self):
    path = self.test_output_path
    statuses = []

    for filename in glob.glob(os.path.join(path, '*.json')):
      print('Evaluating: ' + filename)
      data = json.load(open(filename))
      duration = data['execution_duration']
      if duration > 100000:
        status = 'FAILED'
      else:
        status = 'SUCCESS'

      statuses.append(status)

    self.assertFalse('FAILED' in statuses)

  def test_job_run(self):
    path = self.test_output_path
    statuses = []

    for filename in glob.glob(os.path.join(path, '*.json')):
      print('Evaluating: ' + filename)
      data = json.load(open(filename))
      status = data['state']['result_state']
      statuses.append(status)

    self.assertFalse('FAILED' in statuses)

if __name__ == '__main__':
  out = io.BytesIO()

  unittest.main(testRunner=xmlrunner.XMLTestRunner(output=out),
    failfast=False, buffer=False, catchbreak=False, exit=False)

  with open('TEST-report.xml', 'wb') as report:
    report.write(transform(out.getvalue()))