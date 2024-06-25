from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def organograma(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                niveis_organograma_separador_invalido=False,
                config_organograma_descricao_maior_30_caracteres=False,
                config_organograma_descricao_duplicada=False,
                config_organograma_sem_nivel=False
                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_niveis_organograma_separador_invalido(pre_validacao):
        nomeValidacao = "Níveis de organogramas com separador nulo ou inválido"

        def analisa_niveis_organograma_separador_invalido():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_niveis_organograma_separador_invalido(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_config_organ,
                        i_niveis_organ,
                        separador_nivel,
                        descricao 
                    FROM
                        bethadba.niveis_organ 
                    WHERE 
                        separador_nivel IS NULL  and i_niveis_organ != 1  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(
                            f"Alterado separador do nível: (Configuracao organograma:{row['i_config_organ']}, nivel do organograma {row['i_niveis_organ']}, Descrição: {row['descricao']}) para .")
                        comandoUpdate += f"""UPDATE bethadba.niveis_organ set separador_nivel = '.' where i_config_organ = {row['i_config_organ']} and i_niveis_organ = {row['i_niveis_organ']} and separador_nivel IS NULL;\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_niveis_organograma_separador_invalido: {e}")
            return

        if niveis_organograma_separador_invalido:
            dado = analisa_niveis_organograma_separador_invalido()

            if corrigirErros and len(dado) > 0:
                corrige_niveis_organograma_separador_invalido(listDados=dado)

    def analisa_corrige_config_organograma_descricao_maior_30_caracteres(pre_validacao):
        nomeValidacao = "Configuração de Organograma com descrição possuindo mais de 30 caracteres."

        def analisa_config_organograma_descricao_maior_30_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_config_organograma_descricao_maior_30_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                        SELECT 
                            i_config_organ,
                            descricao,
                            LENGTH(descricao) AS tamanho 
                        FROM 
                            bethadba.config_organ
                        WHERE     
                            tamanho > 30  
                    """)

                if len(busca) > 0:
                    for row in busca:
                        nova_descricao = row['descricao'][:30]
                        dadoAlterado.append(f"Alterada descrição da configuração de organograma: {row['i_config_organ']}-{row['descricao']} para {nova_descricao}")
                        comandoUpdate += f"""UPDATE bethadba.config_organ set descricao = '{nova_descricao}' where i_config_organ = {row['i_config_organ']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_config_organograma_descricao_maior_30_caracteres: {e}")
            return

        if config_organograma_descricao_maior_30_caracteres:
            dado = analisa_config_organograma_descricao_maior_30_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_config_organograma_descricao_maior_30_caracteres(listDados=dado)

    def analisa_corrige_config_organograma_descricao_duplicada(pre_validacao):
        nomeValidacao = "Configuração de Organograma com descrição duplicada."

        def analisa_config_organograma_descricao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_config_organograma_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select
                        list(i_config_organ) as iconfig, 
                        descricao, 
                        count(descricao) AS quantidade 
                    FROM 
                        bethadba.config_organ 
                    GROUP BY 
                        descricao 
                    HAVING 
                        quantidade > 1  
                    """)

                if len(busca) > 0:
                    for row in busca:
                        list_org = row['iconfig'].split(',')
                        for org in list_org[1:]:
                            if len(row['descricao'] + ' ' + org) <= 30:
                                dadoAlterado.append(f"Alterada descrição da configuração de organograma {org}-{row['descricao']} para {row['descricao'] + ' ' + org}")
                                comandoUpdate += f"""UPDATE bethadba.config_organ set descricao = '{row['descricao'] + ' ' + org}' where i_config_organ = {org};\n"""
                            else:
                                sub = row['descricao'][:30 - len(org)] + org
                                dadoAlterado.append(f"Alterada descrição da configuração de organograma {org}-{row['descricao']} para {sub}")
                                comandoUpdate += f"""UPDATE bethadba.config_organ set descricao = '{sub}' where i_config_organ = {org};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_config_organograma_descricao_duplicada: {e}")
            return

        if config_organograma_descricao_duplicada:
            dado = analisa_config_organograma_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_config_organograma_descricao_duplicada(listDados=dado)

    def analisa_corrige_config_organograma_sem_nivel(pre_validacao):
        nomeValidacao = "Configuração do organograma sem nivel informado"

        def analisa_config_organograma_sem_nivel():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_config_organograma_sem_nivel(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select
                        list(i_config_organ) as iconfig, 
                        descricao, 
                        count(descricao) AS quantidade 
                    FROM 
                        bethadba.config_organ 
                    GROUP BY 
                        descricao 
                    HAVING 
                        quantidade > 1  
                    """)

                if len(busca) > 0:
                    for row in busca:
                        list_org = row['iconfig'].split(',')
                        for org in list_org[1:]:
                            if len(row['descricao'] + ' ' + org) <= 30:
                                dadoAlterado.append(f"Alterada descrição da configuração de organograma {org}-{row['descricao']} para {row['descricao'] + ' ' + org}")
                                comandoUpdate += f"""UPDATE bethadba.config_organ set descricao = '{row['descricao'] + ' ' + org}' where i_config_organ = {org};\n"""
                            else:
                                sub = row['descricao'][:30 - len(org)] + org
                                dadoAlterado.append(f"Alterada descrição da configuração de organograma {org}-{row['descricao']} para {sub}")
                                comandoUpdate += f"""UPDATE bethadba.config_organ set descricao = '{sub}' where i_config_organ = {org};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_config_config_organograma_sem_nivel: {e}")
            return

        if config_organograma_sem_nivel:
            dado = analisa_config_organograma_sem_nivel()

            if corrigirErros and len(dado) > 0:
                corrige_config_organograma_sem_nivel(listDados=dado)

    if dadosList:
        analisa_corrige_niveis_organograma_separador_invalido(pre_validacao="niveis_organograma_separador_invalido")
        analisa_corrige_config_organograma_descricao_maior_30_caracteres(pre_validacao="config_organograma_descricao_maior_30_caracteres")
        analisa_corrige_config_organograma_descricao_duplicada(pre_validacao="config_organograma_descricao_duplicada")
        analisa_corrige_config_organograma_sem_nivel(pre_validacao="config_organograma_sem_nivel")
