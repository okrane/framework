'''
Created on 8 Apr 2013

@author: flexsys
'''

from Tkinter import *
import pandas as pd
import numpy as np
import tkFont
  
class Explorer:
    def __init__(self, df):
         
        root = Tk()
        self.bFont = tkFont.Font(family="Times", size=10, weight='bold')
        self.nFont = tkFont.Font(family="Times", size=10, weight='normal')
        
        if hasattr(df, 'name'):
            root.title(df.name)
        else:
            root.title = "Untitled"
            
        self.canvas = Canvas(root, bd=1)
        self.frame = Frame(self.canvas, bd =1)
        self.vsb = Scrollbar(root, orient="vertical", command=self.canvas.yview)
        self.hsb = Scrollbar(root, orient="horizontal", command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=self.vsb.set, xscrollcommand=self.hsb.set)
              
        self.vsb.pack(side="right", fill="y")
        self.hsb.pack(side="bottom", fill="both")
                 
        self.canvas.create_window((4,4), window=self.frame, anchor="nw", tags="self.frame")
        self.canvas.pack(side=LEFT, fill=BOTH, expand=True)
        
#         root.bind("<MouseWheel>", lambda event: self.canvas.yview_scroll(1, UNITS)) 
        root.bind("<Button-4>", lambda event: self.canvas.yview_scroll(-1, UNITS))
        root.bind("<Button-5>", lambda event: self.canvas.yview_scroll(1, UNITS))
        
        self.canvas.focus_set()
        
        self.frame.bind("<Configure>", self.OnFrameConfigure)
         
        self.populate(df)
        
        root.mainloop()
     
    def populate(self, df):
                 
        colnames = df.columns
        rows = min(125,df.shape[0])
        columns = df.shape[1]
                 
        for row in range(rows+1):
            if row == 0:
                for column in range(columns+1):
                    if column == 0:
                        label = Label(self.frame, text="index", bd=1, bg='white', relief = 'raised', font=self.bFont)
                    else:
                        label = Label(self.frame, text=colnames[column-1], bd=1, bg='white', relief = 'raised', font=self.bFont)
                    label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
            else: 
                for column in range(columns+1):
                    if column == 0:
                        label = Label(self.frame, text=df.index[row-1], bd=1, bg='white', font=self.bFont)
                    else:
                        label = Label(self.frame, text=str(df.iloc[row-1,column-1]), bd=1, bg='white', font=self.nFont)
#                     label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
                    label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
         
    def OnFrameConfigure(self, event):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
         
if __name__ == "__main__":    

    df = pd.DataFrame(np.random.randn(100, 3), columns=['a', 'b', 'c'])
    df.name = "test display <series : 'A','B','C'>"
    
    app=Explorer(df)
      
