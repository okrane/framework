


def latex_string(list_image_path,  list_table_string):
    
    for i, value in enumerate(list_image_path):
        list_image_path[i] = value.replace("\\","/")
    
    from datetime import datetime
    list_for_latex = [datetime.strftime(datetime.today(),"%d/%m/%y")]
    list_for_latex.extend(list_image_path[0:13])
    list_for_latex.extend(list_table_string)
    list_for_latex.extend(list_image_path[13:])
    
    
    t = tuple(list_for_latex)
    
    
    my_str = """
    \\documentclass[12pt]{article}
    \\usepackage{graphicx}
    \\usepackage{booktabs}
    \\usepackage{hyperref}
    \\usepackage{geometry}
    \\geometry{a4paper,landscape}
    
    \\author{Algoquant}
    
    \\setlength{\oddsidemargin}{-9pt}
    \\setlength{\evensidemargin}{9pt}  
    
    \\begin{document}
        %s - \\href{mailto:algoquant@keplercheuvreux.com}{algoquant@keplercheuvreux.com}
        \\tableofcontents
        
        
        \\newpage
        \\section{History}
        \\begin{figure}[h!]
          \\includegraphics[width=650pt]{%s}
          \\caption{History}
        \\end{figure}
        
        \\newpage
        \\section{Daily}
            \\begin{figure}[h!]
                \\includegraphics[width=650pt]{%s}
                \\caption{Algo Volume}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Number of occurrences}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Countries}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Intraday algo volume}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
        
        \\newpage
        \\section{Weekly}
            \\begin{figure}[h!]
              \\caption{Algo Volume}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Number of occurrences}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Countries}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Intraday algo volume}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
        
        \\newpage
        \\section{Monthly}
            \\begin{figure}[h!]
              \\caption{Algo Volume}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Number of occurrences}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Countries}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
            \\newpage
            \\begin{figure}[h!]
              \\caption{Intraday algo volume}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
         
        \\newpage    
        \\section{Slippage Daily}
        %s
        
        \\newpage
        \\section{Slippage Monthly}           
        %s
        
        \\newpage
        \\section{Vwap: Weekly Slippage Evolution (from FlexStat)}
            \\begin{figure}[h!]
              \\caption{Vwap slippage evolution}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
        
        \\newpage    
        \\section{Vol: Weekly Slippage Evolution (from FlexStat)}
            \\begin{figure}[h!]
              \\caption{Vol slippage evolution}
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
            
        \\newpage    
        \\section{Dynvol: Weekly Slippage Evolution (from FlexStat)}
            \\begin{figure}[h!]
              \\caption{Dynvol slippage evolution}
              
                \\includegraphics[width=650pt]{%s}
            \\end{figure}
           
        \\newpage 
        \\listoffigures
    
    
    
    
    \\end{document}
    

    """%t
    
    return my_str



if __name__ == "__main__":
    list_image_path = []
    for i in range(16):
        list_image_path.append("C:/temp/Place_from_20131021_to_20131118.png")

    print latex_string(list_image_path, ["Nothing to say", "Nothing to say"])