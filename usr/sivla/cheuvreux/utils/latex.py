'''
Created on Sep 28, 2010

@author: syarc
'''
import os

class CheuvreuxDocument():

    def __init__(self, output):
        self.filename = output
        self._output = open(self.filename, 'w')
        self._header()

    def _header(self):
        print >> self._output, r'\documentclass{article}'
        print >> self._output, r'\usepackage[pdftex]{graphicx}'
        print >> self._output, r'\usepackage{amsmath}'
        print >> self._output, r'\usepackage{bbm}'
        print >> self._output, r'\usepackage{amsfonts}'
        print >> self._output, r'\usepackage{amssymb}'
        print >> self._output, r'\usepackage{geometry}'
        print >> self._output, r'\usepackage{tabularx}'
        print >> self._output, r'\usepackage{colortbl}'
        print >> self._output, r'\usepackage{vmargin}'
        print >> self._output, r'\usepackage{fancyhdr}'
        print >> self._output, r'\usepackage[T1]{fontenc}'
        print >> self._output, r'\usepackage[scaled]{helvet}'
        print >> self._output, r'\setlength{\itemsep}{0pt}'
        print >> self._output, r'\renewcommand*\familydefault{\sfdefault}'
        print >> self._output, r'\setpapersize{USletter}'
        print >> self._output, r'\setmarginsrb{2cm}{1cm}{2cm}{0.5cm}{0.5cm}{0.5cm}{0cm}{0cm}'
        print >> self._output, r'\definecolor{green}{rgb}{0,0.59,0.38}'
        print >> self._output, r'\definecolor{gray}{rgb}{0.5,0.5,0.5}'
        print >> self._output, r'\fancyhead[R]{\includegraphics[height=12pt]{logo.png}}'
        print >> self._output, r'\fancyhead[C]{Study Best Rotation}'
        print >> self._output, r'\fancyhead[L]{\today}'
        print >> self._output, r'\fancyfoot[C]{\color{green}\textbf{\thepage\ -\ www.cheuvreux.com}}'
        print >> self._output, r'\begin{document}'
        print >> self._output, r'\pagestyle{fancy}'

    def end(self):
        print >> self._output, r'\end{document}'

    def add_graphics(self, graphic):
        print >> self._output, r'\includegraphics{%s}' % graphic

    def compile(self):
        self._output.close()
        os.system('pdflatex %s' % self.filename)
