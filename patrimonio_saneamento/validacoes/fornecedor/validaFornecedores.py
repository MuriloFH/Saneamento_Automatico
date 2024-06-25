from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, geraCnpj


def fornecedores(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 fornecedor_cidade_nula=False,
                 cnpj_cpf_nulo=False,
                 cnpj_cpf_duplicado=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def alteraTipoFornecedor():
        list_i_fornec = []
        busca = banco.consultar(f"SELECT i_fornec from bethadba.fornecedores f where f.tipo in ('o', 'O')")
        if len(busca)>0:
            for i in busca:
                list_i_fornec.append(i['i_fornec'])

            list_i_fornec = ', '.join((str(id) for id in list_i_fornec))

            comandoUpdate = f"UPDATE bethadba.fornecedores set tipo = 'J' where i_fornec in ({list_i_fornec});"
            banco.executar(banco.triggerOff(comandoUpdate))
        return

    def analisa_corrige_fornecedor_cidade_nula(pre_validacao):
        nomeValidacao = "Fornecedor sem cidade informada"

        def analisa_fornecedor_cidade_nula():
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

        def corrige_fornecedor_cidade_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT i_cidades from bethadba.parametros p""")

                for dados in listDados:
                    dadoAlterado.append(f"Inserido a cidade {busca[0]['i_cidades']} no fornecedor {dados['i_chave_dsk2']}")
                    comandoUpdate += f"""UPDATE bethadba.fornecedores set i_cidades = {busca[0]['i_cidades']} where i_fornec in ({dados['i_chave_dsk2']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_fornecedor_cidade_nula: {e}")
            return

        if fornecedor_cidade_nula:
            dado = analisa_fornecedor_cidade_nula()

            if corrigirErros and len(dado) > 0:
                corrige_fornecedor_cidade_nula(listDados=dado)

    def analisa_corrige_cnpj_cpf_nulo(pre_validacao):
        nomeValidacao = "Fornecedor sem CNPJ ou CPF informado"

        def analisa_cnpj_cpf_nulo():
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

        def corrige_cnpj_cpf_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []
            i_fornec = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            alteraTipoFornecedor()  # chamando a função, pois caso tenha algum fornecedor com o tipo diferente de F ou J, o fornecedor será definido como juridico.

            for dados in listDados:
                i_fornec.append(dados['i_chave_dsk2'])

            i_fornec = ", ".join(i_fornec)
            try:
                busca = banco.consultar(f"""
                                        SELECT i_fornec, tipo, case f.tipo
                                                                 when 'F' then trim(ISNULL(f.cpf, ''))
                                                                 when 'J' then trim(ISNULL(f.cgc, ''))
                                                                 else ''
                                                                 end as cpfCnpj
                                        FROM bethadba.fornecedores f
                                        where i_fornec in ({i_fornec}) and cpfCnpj = ''   
                                        """)

                if len(busca) > 0:
                    for i in busca:
                        match i['tipo']:
                            case "J":
                                newCnpj = geraCnpj()

                                dadoAlterado.append(f"Adicionado o CNPJ {newCnpj} para o funcionário {i['i_fornec']}")
                                comandoUpdate += f"""UPDATE bethadba.fornecedores set cgc = '{newCnpj}' where i_fornec in ({i['i_fornec']});\n"""
                            case "F":
                                newCpf = geraCfp()

                                dadoAlterado.append(f"Adicionado o CPF {newCpf} para o funcionário {i['i_fornec']}")
                                comandoUpdate += f"""UPDATE bethadba.fornecedores set cpf = '{newCpf}' where i_fornec in ({i['i_fornec']});\n"""
                            case _:
                                comandoUpdate += ""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_cpf_nulo: {e}")
            return

        if cnpj_cpf_nulo:
            dado = analisa_cnpj_cpf_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_cpf_nulo(listDados=dado)

    def analisa_corrige_cnpj_cpf_duplicado(pre_validacao):
        nomeValidacao = "Fornecedor com CNPJ ou CPF duplicados"

        def analisa_cnpj_cpf_duplicado():
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

        def corrige_cnpj_cpf_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                                        SELECT list(i_fornec) as i_fornec,
                                        list(tipo) as tipo,
                                        case f.tipo
                                         when 'F' then trim(ISNULL(f.cpf, ''))
                                         when 'J' then trim(ISNULL(f.cgc, ''))
                                         else ''
                                         end as cpfCnpj
                                        FROM bethadba.fornecedores f
                                        where cpfCnpj != ''
                                        GROUP by cpfCnpj
                                        HAVING COUNT(cpfCnpj) > 1
                                        """)

                if len(busca) > 0:
                    for i in busca:
                        list_i_fornec = i['i_fornec'].split(',')
                        list_tipo = i['tipo'].split(',')

                        for indice in range(0, len(list_i_fornec)):
                            i_fornec = list_i_fornec[indice]
                            tipo = list_tipo[indice]

                            match tipo:
                                case "J":
                                    newCnpj = geraCnpj()

                                    dadoAlterado.append(f"Alterado o CNPJ do funcionário {i_fornec} para {newCnpj}")
                                    comandoUpdate += f"UPDATE bethadba.fornecedores set cgc = '{newCnpj}' where i_fornec = {i_fornec};\n"
                                case "F":
                                    newCpf = geraCfp()

                                    dadoAlterado.append(f"Alterado o CPF do funcionário {i_fornec} para {newCpf}")
                                    comandoUpdate += f"UPDATE bethadba.fornecedores set cpf = '{newCpf}' where i_fornec = {i_fornec};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Patrimonio", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_cnpj_cpf_duplicado: {e}")
            return

        if cnpj_cpf_duplicado:
            dado = analisa_cnpj_cpf_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_cpf_duplicado(listDados=dado)

    if dadosList:
        analisa_corrige_fornecedor_cidade_nula(pre_validacao="fornecedor_cidade_nula")
        analisa_corrige_cnpj_cpf_nulo(pre_validacao='cnpj_cpf_nulo')
        analisa_corrige_cnpj_cpf_duplicado(pre_validacao="cnpj_cpf_duplicado")
    return
