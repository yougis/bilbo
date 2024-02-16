from rasterio.windows import Window
import logging
import numpy as np
from oeilnc_config import settings

logging.info("Utils - Raster Imported")


def block_shapes(src, rows, cols):
    """
    Generator for windows for optimal reading and writing based on the raster format.
    
    Args:
        src (object): The source object representing the raster format.
        rows (int): The number of rows in each window.
        cols (int): The number of columns in each window.
    
    Yields:
        tuple: A tuple representing the window coordinates and dimensions as (xoff, yoff, width, height).
    """
    for i in range(0, src.width, rows):
        if i + rows < src.width:
            num_cols = rows
        else:
            num_cols = src.width - i

        for j in range(0, src.height, cols):
            if j + cols < src.height:
                num_rows = rows
            else:
                num_rows = src.height - j

            yield Window(i, j, num_cols, num_rows)


def changeTypeMaskArrayToUint8(x):
    return x.astype(np.uint8)


def changeTypeMaskArrayToUint16(x):
    return x.astype(np.uint16)