from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, geraCodSiafi


def responsaveis(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 cpf_duplicado=False,
                 cod_siafe_nulo=False,
                 cpf_nulo=False,
                 cpf_invalido=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_cpf_duplicado(pre_validacao):
        nomeValidacao = "Cpf duplicada"

        def analisa_cpf_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT COUNT(cpf), cpf, list(i_respons) as id
                                            from bethadba.responsaveis r
                                            group by r.cpf 
                                            HAVING count(cpf) > 1    
                                         """)

                if len(busca) > 0:
                    for i in busca:
                        a = i['id'].split(',')
                        for j in a[1:]:
                            newCpf = geraCfp()
                            dadoAlterado.append(f"Alterado o cpf do responsável {j}")

                            comandoUpdate += f"""UPDATE bethadba.responsaveis set cpf = '{newCpf}' where i_respons in ({j});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_duplicado: {e}")
            return

        if cpf_duplicado:
            dado = analisa_cpf_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_duplicado(listDados=dado)

    def analisa_corrige_cod_siafe_nulo(pre_validacao):
        nomeValidacao = "cod siafe nulo"

        def analisa_cod_siafe_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cod_siafe_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT COUNT(c.i_cidades), 
                                               list(c.i_cidades) as id_cidade
                                           FROM bethadba.responsaveis r
                                           JOIN bethadba.cidades c ON r.i_cidades = c.i_cidades
                                           WHERE c.cod_siafi IS NULL AND r.i_entidades = 1
                                           group by c.i_cidades
                                           HAVING COUNT(c.i_cidades) > 1  
                                        """)

                if len(busca) > 0:
                    for i in busca:
                        a = i['id_cidade'].split(',')
                        newCodSiafi = geraCodSiafi()
                        comandoUpdate += f"""update bethadba.cidades set cod_siafi = {newCodSiafi} where i_cidades in ({a[0]});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cod_siafe_nulo: {e}")
            return

        if cod_siafe_nulo:
            dado = analisa_cod_siafe_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cod_siafe_nulo(listDados=dado)

    def analisa_corrige_cpf_nulo(pre_validacao):
        nomeValidacao = "CPF nulo"

        def analisa_cpf_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT i_respons, isnull(cpf, '') as cpf from bethadba.responsaveis r where cpf = '' order by i_respons;""")

                if len(busca) > 0:
                    for i in busca:
                        while True:
                            newCpf = geraCfp()
                            if len(newCpf) == 11:
                                break
                        comandoUpdate += f"""update bethadba.responsaveis set cpf = {newCpf} where i_respons = {i['i_respons']};\n"""
                        dadoAlterado.append(f"Inserido o CPF {newCpf} para o responsável {i['i_respons']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_nulo: {e}")
            return

        if cpf_nulo:
            dado = analisa_cpf_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_nulo(listDados=dado)

    def analisa_corrige_cpf_invalido(pre_validacao):
        nomeValidacao = "CPF inválido"

        def analisa_cpf_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                for i in listDados:
                    while True:
                        newCpf = geraCfp()
                        if len(newCpf) == 11:
                            break

                    comandoUpdate += f"""update bethadba.responsaveis set cpf = {newCpf} where i_respons = {i['i_chave_dsk2']};\n"""
                    dadoAlterado.append(f"Inserido o CPF {newCpf} para o responsável {i['i_chave_dsk2']}")

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cpf_invalido: {e}")
            return

        if cpf_invalido:
            dado = analisa_cpf_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_invalido(listDados=dado)

    if dadosList:
        analisa_corrige_cpf_duplicado(pre_validacao="cpf_duplicado")
        analisa_corrige_cod_siafe_nulo(pre_validacao='cod_siafe_nulo')
        analisa_corrige_cpf_nulo(pre_validacao="cpf_nulo")
        analisa_corrige_cpf_invalido(pre_validacao="cpf_invalido")
    return
