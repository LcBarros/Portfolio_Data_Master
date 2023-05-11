from Projeto_IFDATA_Functions import Extract, Transform, Load

#Escopo
    #customiza lista de relatorios a serem extraidos

    #loop a cada item da lista
        #captura dados PF
        #transforma dados PF
        #salva dados PF

        #captura dados PJ
        #transforma dados PJ
        #salva dados PJ

        #captura dados codint
        #transforma dados codint
        #salva dados codint

itens = ["202012", "202009", "202006", "202003"]

def valida_função(etapa, PF, PJ, CODINT):
    if  PF == "NOK" or PJ == "NOK" or CODINT == "NOK":
        print("ERRO" + etapa)
        exit()
    else: return

for i in itens:
    E_pf = Extract(i, "pf")
    E_pj = Extract(i, "pj")
    E_codint = Extract(i, "codint")
    valida_função("Extract", E_pf, E_pj, E_codint)

    T_pf = Transform("pf", E_pf)
    T_pj = Transform("pj", E_pj)
    T_codint = Transform("codint", E_codint)

    L_pf = Load("ifdata_carteira_credito", T_pf)
    L_pj = Load("ifdata_carteira_credito", T_pj)
    L_codint = Load("ifdata_codint", T_codint)

    print(i + " - OK")