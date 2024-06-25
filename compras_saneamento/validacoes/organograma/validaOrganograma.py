from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from datetime import datetime
import polars as pl


def organograma(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                orgao_nao_cadastrado=False,
                unidade_nao_cadastrada=False,
                centro_custo_nao_cadastrado=False
                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_orgao_nao_cadastrado(pre_validacao):
        nomeValidacao = "Orgão não cadastrado para o exercício"

        def analisa_orgao_nao_cadastrado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_orgao_nao_cadastrado(listDados):
            tipoCorrecao = "INCLUSAO"
            comandoInsert = ""
            dadoIncluido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                # busca = analisa_orgao_nao_cadastrado()
                max = banco.consultar("""
                                        select max(reduz_org_ger) as org_max
                                        from compras.orgaos
                """)

                if len(listDados) > 0:
                    df = pl.DataFrame(listDados)
                    df = df.drop('row_nr')
                    df = df.with_row_count()

                    for row in df.iter_rows(named=True):
                        num = row['row_nr'] + 1
                        reduz_org_ger = max[0]['org_max'] + num
                        dadoIncluido.append(f"Incluido Orgão genérico {num}")
                        comandoInsert += f"""insert into compras.orgaos (i_ano, i_orgaos, descricao, administracao, reduz_org_ano, reduz_org_ger, flag, data_alt, deletar, i_entidades)
                         values ({row['i_chave_dsk2']}, {num}, 'ORGAO GENERICO {num}', 'D', {num}, {reduz_org_ger}, {num},  '{datetime.now()}', 0, {row['i_chave_dsk1']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoIncluido)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_orgao_nao_cadastrado: {e}")
            return

        if orgao_nao_cadastrado:
            dado = analisa_orgao_nao_cadastrado()

            if corrigirErros and len(dado) > 0:
                corrige_orgao_nao_cadastrado(listDados=dado)

    def analisa_corrige_unidade_nao_cadastrada(pre_validacao):
        nomeValidacao = "Unidade não cadastrada para o exercício"

        def analisa_unidade_nao_cadastrada():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_unidade_nao_cadastrada(listDados):
            tipoCorrecao = "INCLUSAO"
            comandoInsert = ""
            dadoIncluido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                # busca = analisa_unidade_nao_cadastrada()
                max = banco.consultar("""
                                    select max(reduz_unid_ger) as unid_max
                                    from compras.unidades
                                """)
                if len(listDados) > 0:
                    df = pl.DataFrame(listDados)
                    df = df.drop('row_nr')
                    df = df.with_row_count()
                    for row in df.iter_rows(named=True):
                        num = row['row_nr'] + 1
                        reduz_unid_ger = max[0]['unid_max'] + num
                        dadoIncluido.append(f"Incluida Unidade genérica {num}")
                        comandoInsert += f"""insert into compras.unidades (i_ano, i_orgaos, i_unidades, descricao, reduz_unid_ano, reduz_unid_ger, flag, data_alt, deletar, i_entidades)
                         values ({row['i_chave_dsk2']}, {num}, {num}, 'UNIDADE GENERICA {num}', {num}, {reduz_unid_ger}, {num}, '{datetime.now()}', 0, {row['i_chave_dsk1']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoIncluido)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_unidade_nao_cadastrada: {e}")
            return

        if unidade_nao_cadastrada:
            dado = analisa_unidade_nao_cadastrada()

            if corrigirErros and len(dado) > 0:
                corrige_unidade_nao_cadastrada(listDados=dado)

    def analisa_corrige_centro_custo_nao_cadastrado(pre_validacao):
        nomeValidacao = "Centro de Custo não cadastrado para o exercício"

        def analisa_centro_custo_nao_cadastrado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_centro_custo_nao_cadastrado(listDados):
            tipoCorrecao = "INCLUSAO"
            comandoInsert = ""
            dadoIncluido = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                # busca = analisa_centro_custo_nao_cadastrado()
                max = banco.consultar("""
                                      select max(reduz_cus_ger) as cust_max
                                      from compras.ccustos
                """)
                if len(listDados) > 0:
                    df = pl.DataFrame(listDados)
                    df = df.drop('row_nr')
                    df = df.with_row_count()
                    for row in df.iter_rows(named=True):
                        num = row['row_nr'] + 1
                        reduz_cus_ger = max[0]['cust_max'] + num
                        dadoIncluido.append(f"Incluido Centro de Custos genérico {num}")
                        comandoInsert += f"""insert into compras.ccustos (i_ano, i_ccusto, i_orgaos, i_unidades, cc_nome, reduz_cus_ano, reduz_cus_ger, flag_compras, data_alt, deletar, i_entidades)
                         values ({row['i_chave_dsk2']}, '1', '1', '1', 'CENTRO DE CUSTOS GENERICO {num}', {num}, {reduz_cus_ger}, {num}, '{datetime.now()}', 0, {row['i_chave_dsk1']});\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Compras",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoIncluido)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_centro_custo_nao_cadastrado: {e}")
            return

        if centro_custo_nao_cadastrado:
            dado = analisa_centro_custo_nao_cadastrado()

            if corrigirErros and len(dado) > 0:
                corrige_centro_custo_nao_cadastrado(listDados=dado)

    if dadosList:
        analisa_corrige_orgao_nao_cadastrado(pre_validacao="orgao_nao_cadastrado")
        analisa_corrige_unidade_nao_cadastrada(pre_validacao="unidade_nao_cadastrada")
        analisa_corrige_centro_custo_nao_cadastrado(pre_validacao="centro_custo_nao_cadastrado")
