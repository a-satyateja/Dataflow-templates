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


from __future__ import absolute_import
import logging
import os

import apache_beam as beam
from PIL import Image
from apache_beam.io.gcp import gcsio
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.options.pipeline_options import PipelineOptions
from io import BytesIO
from apache_beam.io import filesystems

class WordExtractingDoFn(beam.DoFn):
    def process(self, element):
        print("reads element ::")
        print(element)
        head, file_name = os.path.split(os.path.splitext(element)[0])
        gcsio_obj = gcsio.GcsIO()
        bufferImg = gcsio_obj.open(element, 'r').read()
        image = Image.open(BytesIO(bufferImg))
        writer = filesystems.FileSystems.create(head + "/" + file_name + ".png", "image/png")
        b = BytesIO()
        image.save(b, format="png")
        contents = b.getvalue()
        writer.write(contents)
        writer.close()
        gcsio_obj.delete(element)
        b.close()

def run(argv=None):
    class WordcountOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_value_provider_argument(
                '--input',
                default='gs://dataflow-samples/shakespeare/kinglear.txt',
                help='Path of the file to read from')

    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)
    wordcount_options = pipeline_options.view_as(WordcountOptions)
    files = (p | 'files' >> MatchFiles(wordcount_options.input)
             | 'read-matches' >> ReadMatches()
             )
    files_and_contents = (files
                          |'read' >> beam.Map(lambda x: x.metadata.path,
                                              ))
    counts = (files_and_contents
              | 'read-1' >> (beam.ParDo(WordExtractingDoFn()))
              )
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
