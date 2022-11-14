# Copyright 2022 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Alex Massen-Hane

import os
import unittest
import PIL
import cairosvg
from numpy import asarray

from academic_observatory_workflows.image import check_image_integrity


class TestWImageVerification(unittest.TestCase):

    def test_check_image_ingetrity(self):
        """Test image verification and conversion."""

        try: 
            # Create mock SVG for testing
            svg_mock = """
                        <svg xmlns="http://www.w3.org/2000/svg" width="4" 
                            height="4" viewBox="0 0 4 4" fill="none" 
                            stroke="#043" stroke-width="1" stroke-linecap="round" 
                            stroke-linejoin="round">
                            <line x1="0" y1="0" x2="4" y2="4"/>
                        </svg>
                    """ 
            
            # Convert mock SVG to PNG 
            cairosvg.svg2png(bytestring=svg_mock,write_to='output.png')

            # Verify conversion
            img = PIL.Image.open('output.png')
            expected_image_array = [ [ [0, 68, 51, 234], [0, 68, 52,  64], [0,  0, 0,    0], [0,  0,  0,   0] ],
                                     [ [0, 68, 51,  60], [0, 68, 51, 233], [0, 70, 50,  66], [0,  0,  0,   0] ],
                                     [ [0,  0,  0,   0], [0, 68, 51,  60], [0, 60, 51, 233], [0, 70, 50,  66] ],
                                     [ [0,  0,  0,   0], [0,  0,  0,   0], [0, 68, 52,  64], [0, 68, 51, 234] ] ]
            self.assertEquals(asarray(expected_image_array).all(), asarray(img).all())

            # Check image integrity (mock is known to be good)
            success = check_image_integrity(image_path='output.png', image_type='png')
            self.assertTrue(success)

        finally:
            # Delete file created by test.
            try:
                os.remove('output.png')
            except:
                raise Exception('Unable to delete output.png from test.')