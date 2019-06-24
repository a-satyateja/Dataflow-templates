#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

from __future__ import absolute_import
import argparse
import logging
import re

from past.builtins import unicode
from apache_beam.io.fileio import MatchFiles
import apache_beam as beam
from PIL import Image
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.io.fileio import MatchFiles, ReadMatches, ReadableFile
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class WordExtractingDoFn(beam.DoFn):

  def process(self, element):
    # readelement = ReadMatches(element)
    print("reads element ::")
    image = Image.open(element)
    image.mode = 'I'
    image.point(lambda i: i * (1. / 256)).convert('L').save('my.jpeg')
    print(element)
    return 1

def run(argv=None):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')

  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')

  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  # lines = p | 'read' >> ReadFromText(known_args.input)

  files = (p | 'files' >> MatchFiles(known_args.input)
             | 'read-matches' >> ReadMatches()
            )
  files_and_contents = (files
                        |'read' >> beam.Map(lambda x: x.metadata.path,
                                              ))
  print('*************************')
  print(files)
  # print(files_and_contents)

  # Count the occurrences of each word.
  def count_ones(word_ones):
    (word, ones) = word_ones
    return (word, sum(ones))

  counts = (files_and_contents
            | 'read-1' >> (beam.ParDo(WordExtractingDoFn()))
            )

  # Format the counts into a PCollection of strings.
  def format_result(word_count):
    (word, count) = word_count
    return '%s: %d' % (word, count)

  # output = counts | 'format' >> beam.Map(format_result)

  # Write the output using a "Write" transform that has side effects.
  # pylint: disable=expression-not-assigned
  # output | 'write' >> WriteToText(known_args.output)

  result = p.run()
  result.wait_until_finish()

  # Do not query metrics when creating a template which doesn't run
  if (not hasattr(result, 'has_job')    # direct runner
      or result.has_job):               # not just a template creation
    empty_lines_filter = MetricsFilter().with_name('empty_lines')
    query_result = result.metrics().query(empty_lines_filter)
    if query_result['counters']:
      empty_lines_counter = query_result['counters'][0]
      logging.info('number of empty lines: %d', empty_lines_counter.result)

    word_lengths_filter = MetricsFilter().with_name('word_len_dist')
    query_result = result.metrics().query(word_lengths_filter)
    if query_result['distributions']:
      word_lengths_dist = query_result['distributions'][0]
      logging.info('average word length: %d', word_lengths_dist.result.mean)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
