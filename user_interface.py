#state variable 0 or 1
run=1

welcome_message="====== ETL covoiturage =====\nBienvenue sur ce projet d'ETL\nSelectionnez ce que vous sohaitez faire :\n1 - Afficher le journal\n2 - Charger un nouveau fichier csv\nQ - Quitter le programmme"

print(welcome_message)
while run==1:

    choice=input()

    if choice=="1":
        run_journal=1
        while run_journal==1:
            try:
                with open("journal.txt", 'r') as journal:
                    journal=journal.read()
                    print(journal)
            except:
                print("Aucun téléchargement")
            q=input("Appuyez sur n'importe quelle touche pour quitter")
            print(welcome_message)
            run_journal=0

    if choice=="2":
        run_ETL=1
        while run_ETL==1:
            print("Entrez l'URL du fichier à télécharger :")
            URL=input()
            URL=str(URL)
            from script import ETL
            ETL(URL)
            
            run_ETL_after=1
            while run_ETL_after==1:
                choice_etl=input("Tapez Q pour quitter ou C pour recommencer :")
                if (choice_etl=="q") or (choice_etl=="Q"):
                    print(welcome_message)
                    run_ETL=0
                    run_ETL_after=0
                if (choice_etl=="c") or (choice_etl=="C"):
                    run_ETL=1
                    run_ETL_after=0
                else:
                    pass
            

    if (choice=="q") or (choice=="Q"):
        run=0
    else:
        print("Tapez 1, 2 ou Q uniquement")