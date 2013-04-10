'''
Created on 6 juil. 2012

@author: gupon
'''


def generate_users_file():
    user_file = open("users.ini", "w")
    
    user_file.write("[users]\n\n")
    
    print "generation du fichier users.ini"
    
    cont = True
    
    while cont:
        print "Nom de l'utilisateur"
        user = raw_input(">")
        
        while user=="":
            user = raw_input(">")
        
        print "veuillez renseigner son mot de passe :"
        pwd = raw_input(">")
        print "Veuillez renseigner son statut t pour trading et s pour supervising"
        statut = raw_input(">")
        
        while statut != "s" and statut != "t":
            print "Le statut renseigne est incorrect :"
            statut = raw_input(">")
        
        print "Veuillez renseigner sa source (ISAM, CSAM,...):"
        source = raw_input(">")
        
        while source=="":
            source = raw_input(">")
        
        if statut == "s":
            statut = "supervising"
        else:
            statut = "trading"
        
        user_file.write(user+"="+pwd+":"+source+":"+statut+":\n")
        
        print "continuer ? y for yes any key for no"
        cont = raw_input(">")=="y"
    
    user_file.close()
    
if __name__ == '__main__':
    
    generate_users_file()