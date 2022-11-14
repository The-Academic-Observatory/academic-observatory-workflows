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

""" Image verification and integrity checking functions """

import os
import logging
from airflow import AirflowException

import PIL
import cairosvg

def check_image_integrity(image_path: str, image_type: str):

    """ Function to check if images are in an accepted type, if they exist and if they are corrupt. 
    Check is performed by verifying and rotating/transposing, and will fail if they are courrpt. 

    PIL can take multiple different file formats, just not SVGs. SVGs are converted 
    to a PNG as required.

    :param image_path: Path to the image to check.
    :param fmt: File format of the image.
    :return valid_file: True if image is okay, otherwise false. 

    """

    # Parameter needed for PIL library to allow large images.
    PIL.Image.MAX_IMAGE_PIXELS = 933120000

    accepted_image_types = ['svg', 'jpg', 'jpeg', 'png', 'webp'] 
    if image_type in accepted_image_types:
        if image_type == 'svg':
            # Convert SVG to PNG
            cairosvg.svg2png(url=image_path, write_to='output.png')
            valid_file = pil_check("output.png")
            os.remove("output.png")
        else:
            valid_file = pil_check(image_path)   
    else:
        valid_file = False
        raise AirflowException(f'Image is neither a {accepted_image_types} : {image_path}')

    return valid_file

def pil_check(image_path: str):

    """Verifies integrity of pictures using PIL (Pillow) library, by verifying and 
    doing a basic transpose and flip.

    :param image_path: Path to the image, e.g. /path/to/file/picture.png
    :return valid_file: Return true if image is okay, otherwise false.
    """

    try:
        valid_file = True
        try: 
            # Open and verify image.
            img = PIL.Image.open(image_path)
            img.verify()
            img.close()
        except:
            logging.error('Unable to do verification on image: ', image_path)
            valid_file = False
        
        try: 
            # Open and do a transpose and flip.
            img = PIL.Image.open(image_path)
            img.transpose(PIL.Image.Transpose.FLIP_LEFT_RIGHT)
            img.close()
        except:
            logging.error('Unable to do transpose on image: ', image_path)
            valid_file = False  

    except:
        valid_file = False
        logging.error("Unable to check the integrity of picture: " + image_path)

    return valid_file