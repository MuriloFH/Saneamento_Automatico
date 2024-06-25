from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraInscricaoEstadual, geraCfp, geraCnpj


def fornecedores(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 cnpj_duplicado=False,
                 cpf_duplicado=False,
                 inscricao_estadual_duplicado=False,
                 cpf_invalido=False,
                 cpf_nulo=False,
                 cnpj_nulo=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    funcionarioList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_cnpj_duplicado(pre_validacao):
        nomeValidacao = "Cnpj duplicado"
        preValidacaoBanco = "cnpj_duplicado"

        def analisa_cnpj_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            listTratado = []
            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    listTratado.append(i)

            if len(listTratado) > 0:
                print(f">> Total de inconsistências encontradas: {len(listTratado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(listTratado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(listTratado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(listTratado)}")

            return listTratado

        def corrige_cnpj_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"

            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:

                        busca = banco.consultar(f"""select i_fornecedores 
                                                from bethadba.fornecedores
                                                where cgccpf = '{dados['i_chave_dsk2']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca[1:]:
                                newCnpj = geraCnpj()

                                dadoAlterado.append(f"Adicionado o  CNPJ {newCnpj} para o fornecedor {i['i_fornecedores']}")
                                comandoUpdate += f"update bethadba.fornecedores set cgccpf = '{newCnpj}' where i_fornecedores in ({i['i_fornecedores']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cnpj_duplicado {e}")

        if cnpj_duplicado:
            dado = analisa_cnpj_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_duplicado(listDados=dado)

    def valida_corrige_cpf_duplicado(pre_validacao):
        nomeValidacao = "Cpf duplicado"
        preValidacaoBanco = "Cpf_duplicado"

        def analisa_cpf_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            fornecedorAnalisado = []
            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    fornecedorAnalisado.append(i)

            if len(fornecedorAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")

            return fornecedorAnalisado

        def corrige_cpf_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"

            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""select i_fornecedores, 
                                                from bethadba.fornecedores
                                                where cgccpf = '{dados['i_chave_dsk2']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca[1:]:
                                newCPF = geraCfp()

                                dadoAlterado.append(f"Adicionado o  CPF {newCPF} para o fornecedor {i['i_fornecedores']}")
                                comandoUpdate += f"update bethadba.fornecedores set cgccpf = '{newCPF}' where i_fornecedores in ({i['i_fornecedores']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na funçao valida_corrige_cpf_duplicado {e}")

        if cpf_duplicado:
            dado = analisa_cpf_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_duplicado(listDados=dado)

    def valida_corrige_inscricao_estadual_duplicado(pre_validacao):
        nomeValidacao = "Inscrição Estadual duplicado"
        preValidacaoBanco = "inscricao_estadual_duplicado"

        def analisa_inscricao_estadual_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            fornecedorAnalisado = []
            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    fornecedorAnalisado.append(i)

            if len(fornecedorAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")

            return fornecedorAnalisado

        def corrige_inscricao_estadual_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""select i_fornecedores
                                                from bethadba.fornecedores
                                                where cgccpf = '{dados['i_chave_dsk2']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca[1:]:
                                newInscricao = geraInscricaoEstadual()

                                dadoAlterado.append(f"Adicionado o  inscrição estadual {newInscricao} para o fornecedor {i['i_fornecedores']}")
                                comandoUpdate += f"update bethadba.fornecedores set inscricao = '{newInscricao}' where i_fornecedores in ({i['i_fornecedores']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_inscricao_estadual_duplicado {e}")

        if inscricao_estadual_duplicado:
            dado = analisa_inscricao_estadual_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_inscricao_estadual_duplicado(listDados=dado)

    def valida_corrige_cpf_invalido(pre_validacao):

        def analisa_cpf_invalido():
            print(">> Iniciando a validação 'CPF inválido' ")
            logSistema.escreveLog(">> Iniciando a validação 'CPF inválido' ")

            fornecedorAnalisado = []

            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    fornecedorAnalisado.append(i)

            if len(fornecedorAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")

            return fornecedorAnalisado

        def corrige_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(">> Iniciando a correção 'CPF inválido' ")
                logSistema.escreveLog(">> Iniciando a correção 'CPF inválido'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""select i_fornecedores, 
                                                    from bethadba.fornecedores
                                                    where cgccpf = '{dados['i_chave_dsk2']}'
                                                    """)
                        if len(busca) > 0:
                            for i in busca:
                                cpfGerado = geraCfp()

                                dadoAlterado.append(f"Alterado o CPF inválido {i['i_chave_dsk2']} para o CPF {cpfGerado} do o fornecedor {i['i_fornecedores']}")
                                comandoUpdate += f"update bethadba.fornecedores set cgccpf = '{cpfGerado}' where i_fornecedores in ({i['i_fornecedores']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(">> Finalizado a correção 'CPF inválido' ")
                    logSistema.escreveLog(">> Finalizado a correção 'CPF inválido' ")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cpf_invalido {e}")

        if cpf_invalido:
            dadoCpfInvalido = analisa_cpf_invalido()

            if corrigirErros and len(dadoCpfInvalido) > 0:
                corrige_cpf_invalido(listDados=dadoCpfInvalido)

    def valida_corrige_cpf_nulo(pre_validacao):
        nomeValidacao = "Cpf nulo"
        preValidacaoBanco = "Cpf_nulo"

        def analisa_cpf_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            fornecedorAnalisado = []
            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    fornecedorAnalisado.append(i)

            if len(fornecedorAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")

            return fornecedorAnalisado

        def corrige_cpf_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    busca = banco.consultar(f"""select i_fornecedores
                                                from bethadba.fornecedores
                                                where cpf is null
                                                """)
                    if len(busca) > 0:
                        for i in busca:
                            newCpf = geraCfp()

                            dadoAlterado.append(f"Adicionado o  CPF {newCpf} para o fornecedor {i['i_fornecedores']}")
                            comandoUpdate += f"update bethadba.fornecedores set cpf = '{newCpf}' where i_fornecedores in ({i['i_fornecedores']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cpf_nulo {e}")

        if cpf_nulo:
            dado = analisa_cpf_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_nulo(listDados=dado)

    def valida_corrige_cnpj_nulo(pre_validacao):
        nomeValidacao = "Cnpj nulo"
        preValidacaoBanco = "cnpj_nulo"

        def analisa_cnpj_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            fornecedorAnalisado = []
            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    fornecedorAnalisado.append(i)

            if len(fornecedorAnalisado) > 0:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(fornecedorAnalisado)}")

            return fornecedorAnalisado

        def corrige_cnpj_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    busca = banco.consultar(f"""select i_fornecedores 
                                                from bethadba.fornecedores
                                                where cgccpf is null
                                                """)
                    if len(busca) > 0:
                        for i in busca:
                            newCnpj = geraCnpj()

                            dadoAlterado.append(f"Adicionado o  CNPJ {newCnpj} para o fornecedor {i['i_fornecedores']}")
                            comandoUpdate += f"update bethadba.fornecedores set cgccpf = '{newCnpj}' where i_fornecedores in ({i['i_fornecedores']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cnpj_nulo {e}")

        if cnpj_nulo:
            dado = analisa_cnpj_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_nulo(listDados=dado)

    if funcionarioList:
        valida_corrige_cnpj_duplicado(pre_validacao='cnpj_duplicado')
        valida_corrige_cpf_duplicado(pre_validacao='cpf_duplicado')
        valida_corrige_inscricao_estadual_duplicado(pre_validacao='inscricao_estadual_duplicado')
        valida_corrige_cpf_invalido(pre_validacao='cpf_invalido')
        valida_corrige_cpf_nulo(pre_validacao='cpf_nulo')
        valida_corrige_cnpj_nulo(pre_validacao='cnpj_nulo')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
    return
