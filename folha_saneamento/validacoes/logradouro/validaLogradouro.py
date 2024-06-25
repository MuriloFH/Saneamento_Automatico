from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def logradouro(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
               corrigirErros=False,
               duplicidade_logradouro=False,
               logradouro_caracter_especial_inicio_descricao=False,
               logradouro_sem_cidade=False,
               logradouro_sem_rua=False
               ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_duplicidade_logradouro(pre_validacao):
        nomeValidacao = "A rua consta com descricao repetidas."

        def analisa_duplicidade_logradouro():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_duplicidade_logradouro(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_ruas) as ruas, 
                        TRIM(nome) as nome,
                        i_cidades, 
                        count(nome) AS quantidade
                    FROM 
                        bethadba.ruas 
                    GROUP BY 
                        nome, 
                        i_cidades
                    HAVING 
                        quantidade > 1    
                 """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        list_ruas = row['ruas'].split(',')
                        for rua in list_ruas:
                            novo_nome = row['nome'] + ' ' + rua
                            dadoAlterado.append(f"Alterado nome da rua {row['nome']} para {novo_nome}")
                            comandoUpdate += f"""UPDATE bethadba.ruas set nome = '{novo_nome}' where i_ruas = {rua};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_duplicidade_logradouro: {e}")
            return

        if duplicidade_logradouro:
            dado = analisa_duplicidade_logradouro()

            if corrigirErros and len(dado) > 0:
                corrige_duplicidade_logradouro(listDados=dado)

    def analisa_corrige_logradouro_caracter_especial_inicio_descricao(pre_validacao):
        nomeValidacao = "Logradouros possuem caracter especial no inicio da descrição"

        def analisa_logradouro_caracter_especial_inicio_descricao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_logradouro_caracter_especial_inicio_descricao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_ruas,
                        nome,
                        SUBSTRING(nome, 1, 1) as nome_com_caracter
                    FROM 
                        bethadba.ruas 
                    WHERE 
                        nome_com_caracter in ('[', ']')    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        descricao_ajustada = str(row['nome']).replace(row['nome_com_caracter'],'')
                        dadoAlterado.append(f"Alterada descrição do Logradouro {row['i_ruas']} de {row['nome']} para {descricao_ajustada}")
                        comandoUpdate += f"""UPDATE bethadba.ruas set nome = '{descricao_ajustada}' where i_ruas = {row['i_ruas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_logradouro_caracter_especial_inicio_descricao: {e}")
            return

        if logradouro_caracter_especial_inicio_descricao:
            dado = analisa_logradouro_caracter_especial_inicio_descricao()

            if corrigirErros and len(dado) > 0:
                corrige_logradouro_caracter_especial_inicio_descricao(listDados=dado)

    def analisa_corrige_logradouro_sem_cidade(pre_validacao):
        nomeValidacao = "Logradouro sem cidade"

        def analisa_logradouro_sem_cidade():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_logradouro_sem_cidade(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_ruas,
                        nome,
                        (SELECT TOP 1 i_cidades FROM bethadba.entidades) as nova_cidade
                    FROM 
                        bethadba.ruas 
                    WHERE 
                        i_cidades IS NULL    
                 """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Alterada cidade do logradouro {row['i_ruas']} - {row['nome']} para {row['nova_cidade']}")
                        comandoUpdate += f"""UPDATE bethadba.ruas set i_cidades = {row['nova_cidade']} where i_ruas = {row['i_ruas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_logradouro_sem_cidade: {e}")
            return

        if logradouro_sem_cidade:
            dado = analisa_logradouro_sem_cidade()

            if corrigirErros and len(dado) > 0:
                corrige_logradouro_sem_cidade(listDados=dado)

    def analisa_corrige_logradouro_sem_rua(pre_validacao):
        nomeValidacao = "Logradouro sem rua"

        def analisa_logradouro_sem_rua():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_logradouro_sem_rua(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""
                                            SELECT i_pessoas, nome_rua, i_ruas, i_cidades
                                            FROM bethadba.pessoas_enderecos 
                                            where nome_rua is not null and i_ruas is null
                                        """)

                codCidadeAtual = None
                buscaRuaCidade = None
                for row in dados:
                    nomeRua = row['nome_rua'].strip()

                    # buscando o i_ruas pelo nome da rua
                    buscaRuaCidade = banco.consultar(f"""SELECT r.i_ruas as iRua
                                                            from bethadba.ruas r
                                                            where r.nome like '%{nomeRua}%'
                                                     """)
                    # caso não retorne pelo nome, busca pelo codigo da cidade
                    if len(buscaRuaCidade) == 0:
                        if row['i_cidades'] is not None:
                            codCidadeAtual = row['i_cidades']

                            buscaRuaCidade = banco.consultar(f"""SELECT min(i_ruas) as iRua
                                                                    from bethadba.ruas r
                                                                    where r.i_cidades = {codCidadeAtual}
                                                            """)
                        # caso ainda não localize nenhuma rua, busca a primeira rua cadastrada na tabela
                        else:
                            buscaRuaCidade = banco.consultar(f"""SELECT min(i_ruas) as iRua
                                                                    from bethadba.ruas r
                                                             """)

                    dadoAlterado.append(f"Adicionado a rua {buscaRuaCidade[0]['iRua']} para o endereço da pessoa {row['i_pessoas']}")
                    comandoUpdate += f"""UPDATE bethadba.pessoas_enderecos set i_ruas = {buscaRuaCidade[0]['iRua']} where i_pessoas = {row['i_pessoas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_logradouro_sem_rua: {e}")
            return

        if logradouro_sem_rua:
            dado = analisa_logradouro_sem_rua()

            if corrigirErros and len(dado) > 0:
                corrige_logradouro_sem_rua(listDados=dado)

    if dadosList:
        analisa_corrige_duplicidade_logradouro(pre_validacao="duplicidade_logradouro")
        analisa_corrige_logradouro_caracter_especial_inicio_descricao(pre_validacao="logradouro_caracter_especial_inicio_descricao")
        analisa_corrige_logradouro_sem_cidade(pre_validacao="logradouro_sem_cidade")
        analisa_corrige_logradouro_sem_rua(pre_validacao="logradouro_sem_rua")
