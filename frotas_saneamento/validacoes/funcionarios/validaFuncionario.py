import datetime as dt
import random

from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, validaCPF


def funcionario(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                cpf_duplicado=False,
                rg_duplicado=False,
                cpf_invalido=False,
                dt_admissao_menor_dt_nascimento=False,
                dt_nascimento_nulo=False,
                cpf_nulo=False):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    funcionarioList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_dt_nascimento_nulo(pre_validacao):

        def analisa_dt_nascimento_nulo():
            print(">> Iniciando a validação 'Campo Data de Nascimento Vazio' ")
            logSistema.escreveLog(">> Iniciando a validação 'Campo Data de Nascimento Vazio' ")
            dadosNulos = []
            for dado in funcionarioList:
                if dado['pre_validacao'] == f'{pre_validacao}':
                    dadosNulos.append(dado)

            if len(dadosNulos) > 0:
                print(f">> Total de inconsistências encontradas: {len(dadosNulos)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dadosNulos)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dadosNulos)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dadosNulos)}")

            return dadosNulos

        def corrige_dt_nascimento_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:
                print(">> Iniciando a correção 'Campo Data de Nascimento Vazio' ")
                logSistema.escreveLog(">> Iniciando a correção 'Campo Data de Nascimento Vazio'")
                codFuncionarios = []
                dadoAlterado = []
                try:
                    busca = banco.consultar(f"""
                                            select i_funcionarios
                                            from bethadba.funcionarios
                                            where nascimento is null
                                            """)
                    if len(busca) > 0:
                        for i in busca:
                            codFuncionarios.append(str(i['i_funcionarios']))
                            dadoAlterado.append(f"Alterado a data de nascimento para 01/01/1901 do funcionário {i['i_funcionarios']}")

                    codFuncionarios = ','.join(codFuncionarios)
                    comandoUpdate = f"update bethadba.funcionarios set nascimento = '1901-01-01' where i_funcionarios in ({codFuncionarios});"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao="funcionarios", preValidacaoBanco="dt_nascimento_nulo", dadoAlterado=dadoAlterado)

                    print(">> Finalizado a correção 'Campo Data de Nascimento Vazio' ")
                    logSistema.escreveLog(">> Finalizado a correção 'Campo Data de Nascimento Vazio' ")
                    return
                except Exception as ex:
                    print(f"Erro na função valida_corrige_dt_nascimento_nulo {ex}")
            else:
                pass

        if dt_nascimento_nulo:
            dadoDtNascimentoNulo = analisa_dt_nascimento_nulo()

            if corrigirErros and len(dadoDtNascimentoNulo) > 0:
                corrige_dt_nascimento_nulo(listDados=dadoDtNascimentoNulo)

    def valida_corrige_cpf_duplicado(pre_validacao):

        def analisa_cpf_duplicado():
            print(">> Iniciando a validação 'CPF duplicado' ")
            logSistema.escreveLog(">> Iniciando a validação 'CPF duplicado' ")

            listFuncTratado = []

            for dado in funcionarioList:
                if dado['pre_validacao'] == f'{pre_validacao}':
                    listFuncTratado.append(dado)

            if len(listFuncTratado) > 0:
                print(f">> Total de inconsistências encontradas: {len(listFuncTratado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(listFuncTratado)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(listFuncTratado)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(listFuncTratado)}")

            return listFuncTratado

        def corrige_cpf_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(">> Iniciando a correção 'CPF duplicado' ")
                logSistema.escreveLog(">> Iniciando a correção 'CPF duplicado'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    buscaDuplicado = banco.consultar(f"""SELECT cpf
                                                        FROM bethadba.funcionarios
                                                        where cpf is not null
                                                        GROUP BY cpf
                                                        HAVING COUNT(*) > 1
                                                        order by cpf""")

                    for duplicado in buscaDuplicado:
                        funcDuplicado = banco.consultar(f"""SELECT * from bethadba.funcionarios f where f.cpf  in ('{duplicado['cpf']}')""")

                        for func in funcDuplicado[1:]:
                            cpfGerado = geraCfp()

                            dadoAlterado.append(f"Gerado o CPF {cpfGerado} para o funcionário {func['i_funcionarios']}")
                            comandoUpdate += f"update bethadba.funcionarios set cpf = '{cpfGerado}' where i_funcionarios in ({func['i_funcionarios']});\n"

                    # print(comandoUpdate)
                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao="funcionarios", preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(">> Finalizado a correção 'CPF duplicado' ")
                    logSistema.escreveLog(">> Finalizado a correção 'CPF duplicado' ")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cpf_duplicado {e}")

        if cpf_duplicado:
            dadoCpfDuplicado = analisa_cpf_duplicado()

            if corrigirErros and len(dadoCpfDuplicado) > 0:
                corrige_cpf_duplicado(listDados=dadoCpfDuplicado)

    def valida_corrige_rg_duplicado(pre_validacao):

        def analisa_rg_duplicado():
            print(">> Iniciando a validação 'RG duplicado' ")
            logSistema.escreveLog(">> Iniciando a validação 'RG duplicado' ")

            funcAnalisados = []
            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    funcAnalisados.append(i)

            if len(funcAnalisados) > 0:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")

            return funcAnalisados

        def corrige_rg_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(">> Iniciando a correção 'RG duplicado' ")
                logSistema.escreveLog(">> Iniciando a correção 'RG duplicado'")

                dadoAlterado = []
                comandoUpdate = ""

                try:

                    buscaDuplicado = banco.consultar(f"""SELECT identidade
                                                            FROM bethadba.funcionarios
                                                            where identidade is not null
                                                            GROUP BY identidade
                                                            HAVING COUNT(*) > 1
                                                            order by identidade""")

                    for duplicado in buscaDuplicado:
                        funcDuplicado = banco.consultar(f"""SELECT * from bethadba.funcionarios f where f.identidade  in ('{duplicado['identidade']}')""")

                        for func in funcDuplicado[1:]:
                            numero_rg = f'{random.randint(1, 28):02}{random.randint(0, 99999999):08}'
                            dadoAlterado.append(f"Gerado o RG {numero_rg} para o funcionário {func['i_funcionarios']}")
                            comandoUpdate += f"update bethadba.funcionarios set identidade = '{numero_rg}' where i_funcionarios in ({func['i_funcionarios']});\n"

                    # print(comandoUpdate)
                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao="funcionarios", preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(">> Finalizado a correção 'RG duplicado' ")
                    logSistema.escreveLog(">> Finalizado a correção 'RG duplicado' ")
                except Exception as e:
                    print(f"Erro na função valida_corrige_rg_duplicado {e}")

        if rg_duplicado:
            dadoCpfDuplicado = analisa_rg_duplicado()

            if corrigirErros and len(dadoCpfDuplicado) > 0:
                corrige_rg_duplicado(listDados=dadoCpfDuplicado)

    def valida_corrige_cpf_invalido(pre_validacao):

        def analisa_cpf_invalido():
            print(">> Iniciando a validação 'CPF inválido' ")
            logSistema.escreveLog(">> Iniciando a validação 'CPF inválido' ")

            funcAnalisados = []

            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    funcAnalisados.append(i)

            if len(funcAnalisados) > 0:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")

            return funcAnalisados

        def corrige_cpf_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(">> Iniciando a correção 'CPF inválido' ")
                logSistema.escreveLog(">> Iniciando a correção 'CPF inválido'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                select i_funcionarios
                                                from bethadba.funcionarios
                                                where i_funcionarios = '{dados['i_chave_dsk4']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                cpfGerado = geraCfp()
                                dadoAlterado.append(f"Alterado o CPF inválido {dados['i_chave_dsk3']} para o CPF {cpfGerado} do funcionário {i['i_funcionarios']}")
                                comandoUpdate += f"update bethadba.funcionarios set cpf = '{cpfGerado}' where i_funcionarios in ({i['i_funcionarios']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao="funcionarios", preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(">> Finalizado a correção 'RG duplicado' ")
                    logSistema.escreveLog(">> Finalizado a correção 'CPF inválido' ")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cpf_invalido {e}")

        if cpf_invalido:
            dadoCpfInvalido = analisa_cpf_invalido()

            if corrigirErros and len(dadoCpfInvalido) > 0:
                corrige_cpf_invalido(listDados=dadoCpfInvalido)

    def valida_dt_admissao_menor_dt_nascimento(pre_validacao):
        nomeValidacao = "data de admissão menor que data de nascimento"
        preValidacaoBanco = "dt_admissao_menor_dt_nascimento"

        def analisa_dt_admissao_menor_dt_nascimento():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            funcAnalisados = []

            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    funcAnalisados.append(i)

            if len(funcAnalisados) > 0:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")

            return funcAnalisados

        def corrige_dt_admissao_menor_dt_nascimento(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                SELECT f.i_funcionarios, 
                                                f.nascimento as dataNascimento
                                                from bethadba.funcionarios f where f.nome = '{dados['i_chave_dsk2']}' 
                                                and f.nascimento != null and f.data_admissao != null 
                                                and f.data_admissao <= f.nascimento
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                newDataAdmissao = dt.datetime.strptime(i['dataNascimento'], '%Y-%m-%d')
                                newDataAdmissao += dt.timedelta(days=18 * 365)

                                dadoAlterado.append(f"Alterado a data de admissao para {newDataAdmissao.strftime('%Y-%m-%d')} do o funcionário {i['i_funcionarios']}")
                                comandoUpdate += f"update bethadba.funcionarios set data_admissao = '{newDataAdmissao.strftime('%Y-%m-%d')}' where i_funcionarios in ({i['i_funcionarios']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao="funcionarios", preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_dt_admissao_menor_dt_nascimento {e}")

        if dt_admissao_menor_dt_nascimento:
            dadoCorrigeDtAdmissaoMaiorDtNascimento = analisa_dt_admissao_menor_dt_nascimento()

            if corrigirErros and len(dadoCorrigeDtAdmissaoMaiorDtNascimento) > 0:
                corrige_dt_admissao_menor_dt_nascimento(listDados=dadoCorrigeDtAdmissaoMaiorDtNascimento)

    def valida_cpf_nulo(pre_validacao):
        nomeValidacao = "CPF nulo"
        preValidacaoBanco = "cpf_nulo"

        def analisa_cpf_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            funcAnalisados = []

            for i in funcionarioList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    funcAnalisados.append(i)

            if len(funcAnalisados) > 0:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(funcAnalisados)}")

            return funcAnalisados

        def corrige_cpf_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        newCpf = geraCfp()

                        busca = banco.consultar(f"""
                                                select i_funcionarios
                                                from bethadba.funcionarios
                                                where nome = '{dados['i_chave_dsk2']}'
                                                and cpf is null
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                dadoAlterado.append(f"Adicionado o CPF {newCpf} para o funcionário {i['i_funcionarios']}")
                                comandoUpdate += f"update bethadba.funcionarios set cpf = '{newCpf}' where i_funcionarios in ({i['i_funcionarios']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao="funcionarios", preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_cpf_nulo {e}")

        if cpf_nulo:
            dadoCpfNulo = analisa_cpf_nulo()

            if corrigirErros and len(dadoCpfNulo) > 0:
                corrige_cpf_nulo(listDados=dadoCpfNulo)

    if funcionarioList:
        valida_corrige_cpf_duplicado(pre_validacao='cpf_duplicado')
        valida_corrige_rg_duplicado(pre_validacao='rg_duplicado')
        valida_corrige_cpf_invalido(pre_validacao='cpf_invalido')
        valida_dt_admissao_menor_dt_nascimento(pre_validacao='dt_admissao_menor_dt_nascimento')
        valida_corrige_dt_nascimento_nulo(pre_validacao='dt_nascimento_nulo')
        valida_cpf_nulo(pre_validacao='cpf_nulo')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)
