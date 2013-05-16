'''
Created on Sep 28, 2010

@author: syarc
'''

from PIL import Image
import matplotlib.pyplot as plt
import os

def save_to_jpg(figure, filename):
    ''' Save a matplotlib figure to a jpg file format

        The MatPlotLib package doesn't support JPG output (some backend does,
        but the output is ugly, so output to PNG and then convert to JPG)

        @param figure Matplotlib figure
        @param filename JPG output filename
    '''

    tmp = 'temp_img.png'
    plt.savefig(tmp)

    output = Image.open(tmp)
    output.save(filename, 'JPEG')
    os.remove(tmp)
