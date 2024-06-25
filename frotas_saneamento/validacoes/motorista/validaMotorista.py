
from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCfp, geraCnh
import datetime as dt


def motoristas(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None, entidade=None,
               corrigirErros=False,
               cpf_nulo=False,
               primeira_cnh_menor_dt_nascimento=False,
               dt_emissao_cnh_menor_primeira_cnh=False,
               dt_emissao_cnh_menor_dt_nascimento=False,
               cnh_duplicado=False
               ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_cpf_nulo(pre_validacao):
        nomeValidacao = "Cpf nulo"
        preValidacaoBanco = "cpf_nulo"

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
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    newCpf = geraCfp()

                    busca = banco.consultar(f"""
                                            select i_funcionarios
                                            from bethadba.funcionarios
                                            where cpf is null
                                            """)
                    if len(busca) > 0:
                        for i in busca:
                            dadoAlterado.append(f"Adicionado o  CPF {newCpf} para o motorista {i['i_funcionarios']}")
                            comandoUpdate += f"update bethadba.funcionarios set cpf = '{newCpf}' where i_funcionarios in ({i['i_funcionarios']});\n"

                        banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cpf_nulo {e}")

        if cpf_nulo:
            dado = analisa_cpf_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cpf_nulo(listDados=dado)

    def valida_corrige_primeira_cnh_menor_dt_nascimento(pre_validacao):
        nomeValidacao = "Primeira cnh menor que data de nascimento"
        preValidacaoBanco = "primeira_cnh_menor_dt_nascimento"

        def analisa_primeira_cnh_menor_dt_nascimento():
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

        def corrige_primeira_cnh_menor_dt_nascimento(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                select i_funcionarios,
                                                nascimento as dataNascimento
                                                from bethadba.funcionarios
                                                where cpf = '{dados['i_chave_dsk1']}'
                                                """)
                        for i in busca:
                            # newDataPrimeiraCnh = dt.datetime.strptime(str(bem['dataNascimento']), '%Y-%m-%d')
                            newDataPrimeiraCnh = i['dataNascimento']
                            newDataPrimeiraCnh += dt.timedelta(days=18 * 365)

                            dadoAlterado.append(f"Alterado a data da primeira Cnh para {str(newDataPrimeiraCnh)[0:10]} do o motorista {i['i_funcionarios']}")
                            comandoUpdate += f"update bethadba.cnh set pri_habilitacao = '{str(newDataPrimeiraCnh)[0:10]}' where i_funcionarios in ({i['i_funcionarios']});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_primeira_cnh_menor_dt_nascimento {e}")

        if primeira_cnh_menor_dt_nascimento:
            dado = analisa_primeira_cnh_menor_dt_nascimento()

            if corrigirErros and len(dado) > 0:
                corrige_primeira_cnh_menor_dt_nascimento(listDados=dado)

    def valida_corrige_dt_emissao_cnh_menor_primeira_cnh(pre_validacao):
        nomeValidacao = "Data emissão da CNH menor que a data da primeira CNH"
        preValidacaoBanco = "dt_emissao_cnh_menor_primeira_cnh"

        def analisa_dt_emissao_cnh_menor_primeira_cnh():
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

        def corrige_dt_emissao_cnh_menor_primeira_cnh(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                SELECT c.pri_habilitacao as dataPrimeiraCnh,
                                                f.i_funcionarios,
                                                f.nascimento as dataNascimento
                                                from bethadba.funcionarios f
                                                join bethadba.cnh c on (c.i_funcionarios = f.i_funcionarios)
                                                where f.cpf = '{dados['i_chave_dsk1']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                # newDataEmissaoCnh = dt.datetime.strptime(bem['dataPrimeiraCnh'], '%Y-%m-%d')
                                newDataEmissaoCnh = i['dataPrimeiraCnh']
                                newDataEmissaoCnh += dt.timedelta(days=1)

                                dadoAlterado.append(f"Alterado a data da CNH vigente para {newDataEmissaoCnh} do o motorista {i['i_funcionarios']}")
                                comandoUpdate += f"update bethadba.cnh set emissao = '{newDataEmissaoCnh}' where i_funcionarios in ({i['i_funcionarios']});\n"

                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_dt_emissao_cnh_menor_primeira_cnh {e}")

        if dt_emissao_cnh_menor_primeira_cnh:
            dado = analisa_dt_emissao_cnh_menor_primeira_cnh()

            if corrigirErros and len(dado) > 0:
                corrige_dt_emissao_cnh_menor_primeira_cnh(listDados=dado)

    def valida_corrige_dt_emissao_cnh_menor_dt_nascimento(pre_validacao):
        nomeValidacao = "Data emissao cnh menor que data de nascimento"
        preValidacaoBanco = "dt_emissao_cnh_menor_dt_nascimento"

        def analisa_dt_emissao_cnh_menor_dt_nascimento():
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

        def corrige_dt_emissao_cnh_menor_dt_nascimento(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    for dados in listDados:
                        busca = banco.consultar(f"""
                                                SELECT c.pri_habilitacao as dataPrimeiraCnh,
                                                f.i_funcionarios,
                                                f.nascimento as dataNascimento
                                                from bethadba.funcionarios f
                                                join bethadba.cnh c on (c.i_funcionarios = f.i_funcionarios)
                                                where f.cpf = '{dados['i_chave_dsk1']}'
                                                """)
                        if len(busca) > 0:
                            for i in busca:
                                # newDataPrimeiraCnh = dt.datetime.strptime(dados['dataNascimento'], '%Y-%m-%d')
                                newDataPrimeiraCnh = dados['dataNascimento']
                                newDataPrimeiraCnh += dt.timedelta(days=18 * 365)

                                dadoAlterado.append(f"Alterado a data da emissao da Cnh para {newDataPrimeiraCnh} do o motorista {dados['i_funcionarios']}")
                                comandoUpdate += f"update bethadba.cnh set emissao = '{newDataPrimeiraCnh}' where i_funcionarios in ({dados['i_funcionarios']});\n"

                            banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_dt_emissao_cnh_menor_dt_nascimento {e}")

        if dt_emissao_cnh_menor_dt_nascimento:
            dado = analisa_dt_emissao_cnh_menor_dt_nascimento()

            if corrigirErros and len(dado) > 0:
                corrige_dt_emissao_cnh_menor_dt_nascimento(listDados=dado)

    def valida_corrige_cnh_duplicado(pre_validacao):
        nomeValidacao = "cnh duplicado"
        preValidacaoBanco = "cnh_duplicado"

        def analisa_cnh_duplicado():
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

        def corrige_cnh_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            if corrigirErros and len(listDados) > 0:

                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                comandoUpdate = ""

                try:
                    busca = banco.consultar(f'''
                                            SELECT list(f.i_funcionarios) as listFunc,
                                            list(c.registro),
                                            count(f.i_funcionarios)
                                            from bethadba.funcionarios f
                                            join bethadba.cnh c on (c.i_funcionarios = f.i_funcionarios)
                                            where c.registro != '' or c.registro != null
                                            GROUP by c.registro
                                            having count(f.i_funcionarios) > 1
                                            ''')
                    if len(busca) > 0:
                        for i in busca:
                            list_i_funcionarios = i['listFunc'].split(',')
                            if len(list_i_funcionarios) > 0:
                                for i_funcionarios in list_i_funcionarios:
                                    newCnh = geraCnh()

                                    dadoAlterado.append(f"Adicionado o numero de CNH {newCnh} para o motorista {i_funcionarios}")
                                    comandoUpdate += f"update bethadba.cnh set registro = '{newCnh}' where i_funcionarios in ({i_funcionarios});\n"

                                banco.executar(comando=banco.triggerOff(comando=comandoUpdate))

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função valida_corrige_cnh_duplicado {e}")

        if cnh_duplicado:
            dado = analisa_cnh_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_cnh_duplicado(listDados=dado)

    if dadosList:
        valida_corrige_cpf_nulo(pre_validacao='cpf_nulo')
        valida_corrige_primeira_cnh_menor_dt_nascimento(pre_validacao='primeira_cnh_menor_dt_nascimento')
        valida_corrige_dt_emissao_cnh_menor_primeira_cnh(pre_validacao='dt_emissao_cnh_menor_primeira_cnh')
        valida_corrige_dt_emissao_cnh_menor_dt_nascimento(pre_validacao='dt_emissao_cnh_menor_dt_nascimento')
        valida_corrige_cnh_duplicado(pre_validacao='cnh_duplicado')

    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    return
