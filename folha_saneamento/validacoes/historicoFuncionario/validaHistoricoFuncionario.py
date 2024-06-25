from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def historicoFuncionario(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                         corrigirErros=False,
                         alteracao_historico_funcionario_maior_data_rescisao=False,
                         funcionario_conta_bancaria_invalida=False,
                         funcionario_com_mais_de_uma_previdencia=False,
                         forma_pagamento_credito_sem_conta_vinculada=False,
                         codigo_esocial_duplicado=False,
                         hist_clt_sem_opcao_federal=False
                         ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_alteracao_historico_funcionario_maior_data_rescisao(pre_validacao):
        nomeValidacao = "Alterações de históricos dos funcionários maior que a data de rescisão."

        def analisa_alteracao_historico_funcionario_maior_data_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_alteracao_historico_funcionario_maior_data_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT
                        hs.i_funcionarios,
                        hs.i_entidades,
                        hs.dt_alteracoes,
                        r.dt_rescisao,
                        date(STRING(r.dt_rescisao, ' ', SUBSTRING(hs.dt_alteracoes, 12, 8))) || ' 00:00:00.000' AS dt_alteracoes_novo
                    FROM
                        bethadba.hist_funcionarios hs
                    INNER JOIN 
                        bethadba.rescisoes r ON (hs.i_funcionarios = r.i_funcionarios AND hs.i_entidades = r.i_entidades)
                    WHERE
                        hs.dt_alteracoes > STRING((select max(s.dt_rescisao) 
                                                   from bethadba.rescisoes s 
                                                   join bethadba.motivos_resc mr on(s.i_motivos_resc = mr.i_motivos_resc)
                                                   where s.i_funcionarios = r.i_funcionarios 
                                                   AND s.i_entidades = r.i_entidades
                                                   and mr.dispensados != 3
                                                   and s.dt_reintegracao is null
                                                   and s.dt_canc_resc is null), ' 23:59:59')       
                    ORDER BY 
                        hs.dt_alteracoes DESC   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada data de alteração histórico do funcionário {row['i_funcionarios']} entidade  {row['i_entidades']} de {row['dt_alteracoes']} para {row['dt_alteracoes_novo']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set dt_alteracoes = '{row['dt_alteracoes_novo']}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_alteracao_historico_funcionario_maior_data_rescisao: {e}")
            return

        if alteracao_historico_funcionario_maior_data_rescisao:
            dado = analisa_alteracao_historico_funcionario_maior_data_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_alteracao_historico_funcionario_maior_data_rescisao(listDados=dado)

    def analisa_corrige_funcionario_conta_bancaria_invalida(pre_validacao):
        nomeValidacao = "Funcionário com conta bancária inválida."

        def analisa_funcionario_conta_bancaria_invalida():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_conta_bancaria_invalida(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        f.i_funcionarios,
                        f.i_entidades,
                        hf.dt_alteracoes,
                        hf.i_bancos AS banco_atual,
                        hf.i_agencias AS agencia_atual,
                        hf.i_pessoas_contas,
                        pc.i_bancos AS banco_novo,
                        pc.i_agencias AS agencia_nova
                    FROM 
                        bethadba.hist_funcionarios hf
                    INNER JOIN 
                        bethadba.funcionarios f ON (hf.i_funcionarios = f.i_funcionarios AND hf.i_entidades = f.i_entidades)
                    INNER JOIN 
                        bethadba.pessoas_contas pc ON (f.i_pessoas = pc.i_pessoas AND pc.i_pessoas_contas = hf.i_pessoas_contas)    
                    WHERE 
                        (pc.i_bancos != hf.i_bancos OR pc.i_agencias != hf.i_agencias) AND hf.forma_pagto = 'R'  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterada conta bancária do histórico do funcionário {row['i_funcionarios']} entidade {row['i_entidades']} data de alteração {row['dt_alteracoes']} para banco {row['banco_novo']} e agencia {row['agencia_nova']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set i_bancos = {row['banco_novo']}, i_agencias = {row['agencia_nova']} where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}' and i_bancos = {row['banco_atual']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_conta_bancaria_invalida: {e}")
            return

        if funcionario_conta_bancaria_invalida:
            dado = analisa_funcionario_conta_bancaria_invalida()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_conta_bancaria_invalida(listDados=dado)

    def analisa_corrige_funcionario_com_mais_de_uma_previdencia(pre_validacao):
        nomeValidacao = "Funcionários com mais de uma previdência informada."

        def analisa_funcionario_com_mais_de_uma_previdencia():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_funcionario_com_mais_de_uma_previdencia(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_funcionarios,
                        i_entidades,
                        dt_alteracoes,
                        prev_federal,
                        prev_estadual,
                        fundo_prev,
                        LENGTH(REPLACE(prev_federal || prev_estadual || fundo_prev, 'N', '')) AS quantidade
                    FROM 
                        bethadba.hist_funcionarios
                    WHERE
                        quantidade > 1  
                 """)

                if len(busca) > 0:
                    for row in busca:

                        dadoAlterado.append(f"Alterado histórico do funcionário {row['i_funcionarios']} entidade {row['i_entidades']} data de alteração {row['dt_alteracoes']} para ter somente uma previdencia federal.")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set prev_federal = 'S', prev_estadual = 'N', fundo_prev = 'N' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_funcionario_com_mais_de_uma_previdencia: {e}")
            return

        if funcionario_com_mais_de_uma_previdencia:
            dado = analisa_funcionario_com_mais_de_uma_previdencia()

            if corrigirErros and len(dado) > 0:
                corrige_funcionario_com_mais_de_uma_previdencia(listDados=dado)

    def analisa_corrige_forma_pagamento_credito_sem_conta_vinculada(pre_validacao):
        nomeValidacao = "Histórico do Funcionário com forma de pagamento Crédito porém sem conta vinculada."

        def analisa_forma_pagamento_credito_sem_conta_vinculada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_forma_pagamento_credito_sem_conta_vinculada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_funcionarios,
                        i_entidades,
                        dt_alteracoes,
                        i_pessoas_contas 
                    FROM 
                        bethadba.hist_funcionarios
                    WHERE 
                        forma_pagto = 'R' 
                        AND i_pessoas_contas IS NULL
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado forma de pagamento para D no histórico do funcionário  {row['i_funcionarios']} entidade {row['i_entidades']} data de alteração {row['dt_alteracoes']}")
                        comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set forma_pagto = 'D' where i_funcionarios = {row['i_funcionarios']} and i_entidades = {row['i_entidades']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_forma_pagamento_credito_sem_conta_vinculada: {e}")
            return

        if forma_pagamento_credito_sem_conta_vinculada:
            dado = analisa_forma_pagamento_credito_sem_conta_vinculada()

            if corrigirErros and len(dado) > 0:
                corrige_forma_pagamento_credito_sem_conta_vinculada(listDados=dado)

    def analisa_corrige_codigo_esocial_duplicado(pre_validacao):
        nomeValidacao = "Código eSocial duplicado"

        def analisa_codigo_esocial_duplicado():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_codigo_esocial_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""SELECT
                                                a.i_entidades as entidades,
                                                list(i_funcionarios) as funcionarios,
                                                a.codigo_esocial as codigo_esocial,
                                                COUNT(codigo_esocial) AS total 
                                            FROM 
                                                   bethadba.funcionarios as a                         
                                            GROUP BY 
                                               entidades,
                                                codigo_esocial
                                            HAVING 
                                                total > 1
                                        """)

                for row in dados:
                    listFuncionarios = row['funcionarios'].split(',')

                    numEsocial = row['codigo_esocial']
                    parts = numEsocial.split('-')
                    number = parts[1]

                    for func in listFuncionarios:
                        newCodEsocial = f"{row['entidades']}-{number}-{func}-{row['entidades']}"

                        dadoAlterado.append(f"Adicionado o codigo eSocial {newCodEsocial} para o funcionário {func} da entidade {row['entidades']}")
                        comandoUpdate += f"""UPDATE bethadba.funcionarios set codigo_esocial = '{newCodEsocial}' where i_entidades = {row['entidades']} and i_funcionarios in ({func});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_codigo_esocial_duplicado: {e}")
            return

        if codigo_esocial_duplicado:
            dado = analisa_codigo_esocial_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_codigo_esocial_duplicado(listDados=dado)

    def analisa_corrige_hist_clt_sem_opcao_federal(pre_validacao):
        nomeValidacao = "Historico com vínculo CLR com a opção Federal marcada"

        def analisa_hist_clt_sem_opcao_federal():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_hist_clt_sem_opcao_federal(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""    
                                            select
                                                distinct hist_funcionarios.i_entidades ,
                                                hist_funcionarios.i_funcionarios as func,
                                                hist_funcionarios.dt_alteracoes,
                                                tipo_vinculo,
                                                prev_federal,
                                                categoria_esocial,
                                                funcionarios.tipo_func
                                            from
                                                bethadba.hist_funcionarios,
                                                bethadba.vinculos,
                                                bethadba.funcionarios
                                            where
                                                hist_funcionarios.i_vinculos = vinculos.i_vinculos and 
                                                hist_funcionarios.i_funcionarios = funcionarios.i_funcionarios and 
                                                hist_funcionarios.i_entidades = funcionarios.i_entidades and
                                                tipo_vinculo = 1 and 
                                                prev_federal = 'N' and 
                                                categoria_esocial not in (901)
                                                and funcionarios.tipo_func not in ('A')
                                        """)

                for row in dados:
                    dadoAlterado.append(f"""Marcado o campo prev_federal do historico com data {row['dt_alteracoes']} do funcionário {row['func']} da entidade {row['i_entidades']}""")
                    comandoUpdate += f"""UPDATE bethadba.hist_funcionarios set prev_federal = 'S' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['func']} and dt_alteracoes = '{row['dt_alteracoes']}';\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_hist_clt_sem_opcao_federal: {e}")
            return

        if hist_clt_sem_opcao_federal:
            dado = analisa_hist_clt_sem_opcao_federal()

            if corrigirErros and len(dado) > 0:
                corrige_hist_clt_sem_opcao_federal(listDados=dado)

    if dadosList:
        analisa_corrige_alteracao_historico_funcionario_maior_data_rescisao(pre_validacao="alteracao_historico_funcionario_maior_data_rescisao")
        analisa_corrige_funcionario_conta_bancaria_invalida(pre_validacao="funcionario_conta_bancaria_invalida")
        analisa_corrige_funcionario_com_mais_de_uma_previdencia(pre_validacao="funcionario_com_mais_de_uma_previdencia")
        analisa_corrige_forma_pagamento_credito_sem_conta_vinculada(pre_validacao="forma_pagamento_credito_sem_conta_vinculada")
        analisa_corrige_codigo_esocial_duplicado(pre_validacao='codigo_esocial_duplicado')
        analisa_corrige_hist_clt_sem_opcao_federal(pre_validacao='hist_clt_sem_opcao_federal')
