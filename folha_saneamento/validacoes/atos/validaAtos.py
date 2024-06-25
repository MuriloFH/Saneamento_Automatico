from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def atos(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
         corrigirErros=False,
         numero_nulo=False,
         duplicidade_numero_tipo_ato=False,
         duplicidade_descricao_tipo_ato=False,
         natureza_texto_juridico_nula=False,
         data_fonte_divulgacao_menor_data_publicacao_ato=False,
         ato_data_inicial_nulo=False
         ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_numero_nulo(pre_validacao):
        nomeValidacao = "Ato com número nulo."

        def analisa_numero_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_numero_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_atos,
                        'NAO INFOR. ' || i_atos as novo_num
                    FROM 
                        bethadba.atos 
                    WHERE
                        num_ato IS NULL OR num_ato = ''   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterado numero do ato {row['i_atos']} de nulo para {row['novo_num']}")
                        comandoUpdate += f"""UPDATE bethadba.atos set num_ato = '{row['novo_num']}' where i_atos = {row['i_atos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_numero_nulo: {e}")
            return

        if numero_nulo:
            dado = analisa_numero_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_numero_nulo(listDados=dado)

    def analisa_corrige_duplicidade_numero_tipo_ato(pre_validacao):
        nomeValidacao = "Atos com numero duplicado possuindo mesmo tipo."

        def analisa_duplicidade_numero_tipo_ato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_duplicidade_numero_tipo_ato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_atos) as idatos,
                        trim(num_ato) as num_ato,
                        i_tipos_atos,
                        count(num_ato) AS quantidade
                    FROM 
                        bethadba.atos 
                    GROUP BY 
                        num_ato,
                        i_tipos_atos 
                    HAVING 
                        quantidade > 1   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_atos = row['idatos'].split(',')
                        for ato in list_atos[1:]:
                            if len(row['num_ato'] + ' ' + ato) <= 16:
                                dadoAlterado.append(f"Alterado numero do ato {ato} de  {row['num_ato']} para {row['num_ato'] + ' ' + ato}")
                                comandoUpdate += f"""UPDATE bethadba.atos set num_ato = '{row['num_ato'] + ' ' + ato}' where i_atos = {ato};\n"""
                            else:
                                sub = row['num_ato'][:16 - len(ato)] + ato
                                dadoAlterado.append(f"Alterado numero do ato {ato} de {row['num_ato']} para {sub}")
                                comandoUpdate += f"""UPDATE bethadba.atos set num_ato = '{sub}' where i_atos = {ato};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_duplicidade_numero_tipo_ato: {e}")
            return

        if duplicidade_numero_tipo_ato:
            dado = analisa_duplicidade_numero_tipo_ato()

            if corrigirErros and len(dado) > 0:
                corrige_duplicidade_numero_tipo_ato(listDados=dado)

    def analisa_corrige_duplicidade_descricao_tipo_ato(pre_validacao):
        nomeValidacao = "Tipos de Atos com descrição duplicada."

        def analisa_duplicidade_descricao_tipo_ato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_duplicidade_descricao_tipo_ato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_tipos_atos) as tipos_ato, 
                        nome,
                        count(nome) AS quantidade 
                    FROM 
                        bethadba.tipos_atos 
                    GROUP BY 
                        nome 
                    HAVING
                        quantidade > 1  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_atos = row['tipos_ato'].split(',')
                        for ato in list_atos[1:]:
                            if len(row['nome'] + ' ' + ato) <= 40:
                                dadoAlterado.append(f"Alterada descrição do tipo do ato {ato}-{row['nome']} para {row['nome'] + ' ' + ato}")
                                comandoUpdate += f"""UPDATE bethadba.tipos_atos set nome = '{row['nome'] + ' ' + ato}' where i_tipos_atos = {ato};\n"""
                            else:
                                sub = row['nome'][:40 - len(ato)] + ato
                                dadoAlterado.append(f"Alterada descrição do tipo do ato {ato}-{row['nome']} para {sub}")
                                comandoUpdate += f"""UPDATE bethadba.tipos_atos set nome = '{sub}' where i_tipos_atos = {ato};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_duplicidade_descricao_tipo_ato: {e}")
            return

        if duplicidade_descricao_tipo_ato:
            dado = analisa_duplicidade_descricao_tipo_ato()

            if corrigirErros and len(dado) > 0:
                corrige_duplicidade_descricao_tipo_ato(listDados=dado)

    def analisa_corrige_natureza_texto_juridico_nula(pre_validacao):
        nomeValidacao = "Ato com natureza do texto jurídico nula"

        def analisa_natureza_texto_juridico_nula():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_natureza_texto_juridico_nula(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                       SELECT 
                           i_atos,
                           num_ato,
                           (SELECT TOP 1 i_natureza_texto_juridico FROM bethadba.atos where i_natureza_texto_juridico is not null GROUP BY i_natureza_texto_juridico ORDER BY COUNT(i_natureza_texto_juridico) DESC) as natureza_texto
                        FROM 
                            bethadba.atos 
                        WHERE 
                            i_natureza_texto_juridico IS NULL  
                    """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada natureza do texto juridico do ato {row['i_atos']}-{row['num_ato']} para {row['natureza_texto']}")
                        comandoUpdate += f"""UPDATE bethadba.atos set i_natureza_texto_juridico = '{row['natureza_texto']}' where i_atos = {row['i_atos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_natureza_texto_juridico_nula: {e}")
            return

        if natureza_texto_juridico_nula:
            dado = analisa_natureza_texto_juridico_nula()

            if corrigirErros and len(dado) > 0:
                corrige_natureza_texto_juridico_nula(listDados=dado)

    def analisa_corrige_data_fonte_divulgacao_menor_data_publicacao_ato(pre_validacao):
        nomeValidacao = "Data da fonte de divulgação é menor que a data de publicação do ato."

        def analisa_data_fonte_divulgacao_menor_data_publicacao_ato():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_data_fonte_divulgacao_menor_data_publicacao_ato(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                       SELECT 
                            a.i_atos,
                            fa.dt_publicacao as dt_publicacao_fonte,
                            a.dt_publicacao as  dt_publicacao_ato      
                        FROM 
                            bethadba.atos a
                        INNER JOIN 
                            bethadba.fontes_atos fa ON (fa.i_atos = a.i_atos)
                        WHERE 
                            fa.dt_publicacao < a.dt_publicacao  
                    """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data de publicacao da fonte do ato {row['i_atos']} de {row['dt_publicacao_fonte']} para {row['dt_publicacao_ato']}")
                        comandoUpdate += f"""UPDATE bethadba.fontes_atos set dt_publicacao = '{row['dt_publicacao_ato']}' where i_atos = {row['i_atos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_data_fonte_divulgacao_menor_data_publicacao_ato: {e}")
            return

        if data_fonte_divulgacao_menor_data_publicacao_ato:
            dado = analisa_data_fonte_divulgacao_menor_data_publicacao_ato()

            if corrigirErros and len(dado) > 0:
                corrige_data_fonte_divulgacao_menor_data_publicacao_ato(listDados=dado)

    def analisa_corrige_ato_data_inicial_nulo(pre_validacao):
        nomeValidacao = "Ato com data inicial nulo."

        def analisa_ato_data_inicial_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_ato_data_inicial_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_atos, 
                        dt_vigorar 
                    FROM
                        bethadba.atos 
                    WHERE
                        dt_inicial IS NULL 
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada data inicial do ato  {row['i_atos']} de nulo para {row['dt_vigorar']}")
                        comandoUpdate += f"""UPDATE bethadba.atos set dt_inicial = '{row['dt_vigorar']}' where i_atos = {row['i_atos']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_ato_data_inicial_nulo: {e}")
            return

        if ato_data_inicial_nulo:
            dado = analisa_ato_data_inicial_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_ato_data_inicial_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_numero_nulo(pre_validacao="numero_nulo")
        analisa_corrige_duplicidade_numero_tipo_ato(pre_validacao="duplicidade_numero_tipo_ato")
        analisa_corrige_duplicidade_descricao_tipo_ato(pre_validacao="duplicidade_descricao_tipo_ato")
        analisa_corrige_natureza_texto_juridico_nula(pre_validacao="natureza_texto_juridico_nula")
        analisa_corrige_data_fonte_divulgacao_menor_data_publicacao_ato(pre_validacao="data_fonte_divulgacao_menor_data_publicacao_ato")
        analisa_corrige_ato_data_inicial_nulo(pre_validacao="ato_data_inicial_nulo")
